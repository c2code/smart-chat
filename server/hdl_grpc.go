/******************************************************************************
 *
 *  Description :
 *
 *    Handler of gRPC connections. See also hdl_websock.go for websockets and
 *    hdl_longpoll.go for long polling.
 *
 *****************************************************************************/

package main

import (
	"crypto/tls"
	"io"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"hexmeet.com/beluga/chat/pbx"
	"hexmeet.com/beluga/chat/server/logp"
	"hexmeet.com/beluga/chat/server/logp/configure"
)

type grpcNodeServer struct {
}

func (sess *Session) closeGrpc() {
	if sess.proto == GRPC {
		sess.lock.Lock()
		sess.grpcnode = nil
		sess.lock.Unlock()
	}
}

// Equivalent of starting a new session and a read loop in one
func (*grpcNodeServer) MessageLoop(stream pbx.Node_MessageLoopServer) error {
	sess, _ := globals.sessionStore.NewSession(stream, "")
	defer TryLogDump()
	globals.logger.Infof("grpc: new session id %s", sess.sid)
	defer func() {
		sess.closeGrpc()
		sess.cleanUp(false)
	}()

	go sess.writeGrpcLoop()

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			sess.logger.Infof("grpc: recv sess.sid=%s,sess.proto=%u,err=%s", sess.sid, sess.proto, err.Error())
			return err
		}
		sess.logger.Infof("grpc in:,sid=%s,proto=%d", truncateStringIfTooLong(in.String()), sess.sid, sess.proto)
		sess.dispatch(pbCliDeserialize(in))

		sess.lock.Lock()
		if sess.grpcnode == nil {
			sess.lock.Unlock()
			break
		}
		sess.lock.Unlock()
	}

	return nil
}

func (sess *Session) writeGrpcLoop() {
	defer func() {
		sess.closeGrpc() // exit MessageLoop
		TryLogDump()
	}()

	for {
		select {
		case msg, ok := <-sess.send:
			if !ok {
				// channel closed
				return
			}
			//msg_bytes := msg.([]byte)
			//sess.logger.Infof("send:,sid=,proto=,msg=",sess.sid,sess.proto,msg)
			//sess.logger.Info("send:",msg.(*pbx.ServerMsg))
			if err := grpcWrite(sess, msg); err != nil {
				sess.logger.Infof("grpc: write %s %s", sess.sid, err.Error())
				return
			}
		case msg := <-sess.stop:
			// Shutdown requested, don't care if the message is delivered
			if msg != nil {
				grpcWrite(sess, msg)
			}
			return

		case topic := <-sess.detach:
			sess.delSub(topic)
		}
	}
}

func grpcWrite(sess *Session, msg interface{}) error {
	out := sess.grpcnode
	if out != nil {
		// Will panic if msg is not of *pbx.ServerMsg type. This is an intentional panic.
		return out.Send(msg.(*pbx.ServerMsg))
	}
	return nil
}

func serveGrpc(addr string, tlsConf *tls.Config) (*grpc.Server, error) {
	if addr == "" {
		return nil, nil
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	secure := ""
	var opts []grpc.ServerOption
	opts = append(opts, grpc.MaxRecvMsgSize(int(globals.maxMessageSize)))
	if tlsConf != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConf)))
		secure = " secure"
	}
	srv := grpc.NewServer(opts...)
	pbx.RegisterNodeServer(srv, &grpcNodeServer{})
	configure.Logging("beluga")
	log := logp.NewLogger("serveGrpc")
	log.Infof("gRPC/%s%s server is registered at [%s]", grpc.Version, secure, addr)

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Infof("gRPC server failed: %s", err.Error())
		}
	}()

	return srv, nil
}

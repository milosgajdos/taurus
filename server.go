package taurus

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/gorilla/mux"
)

// Api provides Taurus framework HTTP API
type Api struct {
	httpserver *http.Server
	listener   net.Listener
}

// ApiConfig allows to define Taurus API configuration
type ApiConfig struct {
	// API server bind address
	Address string
	// TLS configuration
	TlsConfig *tls.Config
	// Taurus framework Job store
	Store Store
}

type Context struct {
	store Store
}

func newRouter(c *Context) *mux.Router {
	r := mux.NewRouter()
	routeMap := Routes()

	for method, routes := range routeMap[APIVERSION] {
		for route, rhandler := range routes {
			log.Printf("Registering HTTP route. Method: %s, Path: %s", method, route)
			// local scope for http.Handler
			rh := rhandler
			wrapHandleFunc := func(w http.ResponseWriter, r *http.Request) {
				log.Printf("%s\t%s", r.Method, r.RequestURI)
				rh(c, w, r)
			}
			r.Path("/" + APIVERSION + route).Methods(method).HandlerFunc(wrapHandleFunc)
			r.Path(route).Methods(method).HandlerFunc(wrapHandleFunc)
		}
	}

	return r
}

func newListener(proto, addr string, tlsConfig *tls.Config) (net.Listener, error) {
	var (
		l   net.Listener
		err error
	)

	switch proto {
	case "unix", "unixpacket":
		// Unix sockets must be unlink()ed before being reused again
		if err := syscall.Unlink(addr); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		l, err = net.Listen(proto, addr)
	case "tcp":
		l, err = net.Listen(proto, addr)
	default:
		return nil, fmt.Errorf("unsupported protocol: %q", proto)
	}

	if tlsConfig != nil {
		tlsConfig.NextProtos = []string{"http/1.1"}
		l = tls.NewListener(l, tlsConfig)
	}

	return l, err
}

// ListenAndServe start API server
//
// ListenAndServe blocks until http server returns error
// Due to its blocking behaviour this function should be run in its own goroutines
func (a *Api) ListenAndServe() error {
	return a.httpserver.Serve(a.listener)
}

// Creates and configures API server with provided configuration
//
// NewApi crates and initializes Api server with provided configuration
// It returns error if either configuration is invalid or if API server could not
// be created
func NewApi(c *ApiConfig) (*Api, error) {
	ctx := &Context{
		store: c.Store,
	}
	api := newRouter(ctx)
	server := &http.Server{
		Handler: api,
	}

	protoAddrParts := strings.SplitN(c.Address, "://", 2)
	if len(protoAddrParts) == 1 {
		protoAddrParts = []string{"tcp", protoAddrParts[0]}
	}

	listener, err := newListener(protoAddrParts[0], protoAddrParts[1], c.TlsConfig)
	if err != nil {
		return nil, err
	}
	server.Addr = protoAddrParts[1]

	return &Api{
		httpserver: server,
		listener:   listener,
	}, nil
}

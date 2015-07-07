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

type Api struct {
	httpserver *http.Server
	listener   net.Listener
}

type ApiConfig struct {
	Address   string
	TlsConfig *tls.Config
	Store     Store
	Master    string
}

type Context struct {
	store  Store
	master string
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

func (a *Api) ListenAndServe() error {
	return a.httpserver.Serve(a.listener)
}

func NewApi(c *ApiConfig) (*Api, error) {
	ctx := &Context{
		store:  c.Store,
		master: c.Master,
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

//func doomJobMarker(store Store, driver scheduler.SchedulerDriver, done chan struct{}) {
//	for {
//		jobs, err := store.GetJobs(Doomed)
//		if err != nil {
//			// if we can't read New Jobs in we must panic
//			panic(err)
//		}
//
//		if len(jobs) == 0 {
//			// wait 5 seconds before attempting another scan
//			<-time.After(time.Second * 5)
//			continue
//		}
//
//		for _, job := range jobs {
//			log.Printf("Killing running tasks for Job: %s", job.Id)
//			//TODO: start killing the running tasks here
//			// - Get a list of running tasks from Master
//			// - if the list is empty, remove the job from catalog
//			// - Kill task, pull from Running queue
//			for _, taskId := range taskIds {
//				err, task := store.GetTask("running", taskId)
//				if err != nil {
//					if serr, ok := err.(*StoreError); ok {
//						if serr.Code != ErrNotFound {
//							log.Printf("Error pulling task %s from running queue: %s", taskId, err)
//							continue
//						}
//					}
//				}
//				mesosStatus, err := driver.KillTask(task.Taskid)
//				if err != nil {
//					log.Printf("Mesos status: %#v Failed to kill Task %s: %s", mesosStatus, taskId, err)
//				}
//			}
//			if job.Task.Replicas != 0 {
//				job.Task.Replicas = 0
//				if err := store.UpdateJob(job); err != nil {
//					// if we can't update Job in we must panic
//					panic(err)
//				}
//			}
//		}
//	}
//}

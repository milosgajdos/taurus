**THIS PROJECT IS BROKEN AND UNSTABLE. CONSIDER YOURSELF WARNED.**

# taurus
Simple Mesos Container framework written in Go.

At the moment this is just a **PoC** serving for learning purpose. I'm not sure if I turn into more than just that.
There is a lot of design flaws, lots of existing code would need to be refactored.

## Theory of operation
**taurus** provides REST API that allows you to run Mesos tasks in Docker containers.

You can run **taurus** using docker-compose.

**taurus** uses NATS distributed queue to queue pending and doomed tasks. You can implement your own queue that satisfies ```queue``` taurus Go interface and plug it into the framework.

**taurus** ships with local k/v basic store implementation based on Couchbase's local-gkvlite storage. You can implement your own storage that satisfies ```store``` taurus Go interface and plug it into the framework.

## Example
There are some example jobs in ```examplejobs``` directory.

Submit a job:
```
$ curl -i -X POST 127.0.0.1:8080/job/new -d @job.json
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8
Date: Sun, 12 Jul 2015 22:53:17 GMT
Content-Length: 0
```

Taurus logs:
```
2015/07/12 23:53:17 server.go:43: POST	/job/new
2015/07/12 23:53:17 api.go:103: Submitting Job taurusjob
2015/07/12 23:53:17 scheduler.go:315: No tasks to kill
2015/07/12 23:53:17 scheduler.go:381: Launched tasks: map[string]string{}
2015/07/12 23:53:17 scheduler.go:568: No Pending tasks available
2015/07/12 23:53:18 scheduler.go:159: Creating new Pending task taurusjob-web-prod-1436741598-c9cbf28d-28e8-11e5-a327-600308a80bb2 for job taurusjob
2015/07/12 23:53:18 scheduler.go:159: Creating new Pending task taurusjob-web-prod-1436741598-c9cc085d-28e8-11e5-a327-600308a80bb2 for job taurusjob
2015/07/12 23:53:18 scheduler.go:159: Creating new Pending task taurusjob-web-prod-1436741598-c9cc0d1c-28e8-11e5-a327-600308a80bb2 for job taurusjob
2015/07/12 23:53:18 scheduler.go:159: Creating new Pending task taurusjob-web-prod-1436741598-c9cc10ea-28e8-11e5-a327-600308a80bb2 for job taurusjob
2015/07/12 23:53:18 scheduler.go:203: Queueing task taurusjob-web-prod-1436741598-c9cbf28d-28e8-11e5-a327-600308a80bb2 to Pending queue
2015/07/12 23:53:18 scheduler.go:203: Queueing task taurusjob-web-prod-1436741598-c9cc085d-28e8-11e5-a327-600308a80bb2 to Pending queue
2015/07/12 23:53:18 scheduler.go:203: Queueing task taurusjob-web-prod-1436741598-c9cc0d1c-28e8-11e5-a327-600308a80bb2 to Pending queue
2015/07/12 23:53:18 scheduler.go:203: Queueing task taurusjob-web-prod-1436741598-c9cc10ea-28e8-11e5-a327-600308a80bb2 to Pending queue
...
...
2015/07/12 23:53:26 scheduler.go:667: Task taurusjob-web-prod-1436741598-c9cc0d1c-28e8-11e5-a327-600308a80bb2 is in state TASK_RUNNING
2015/07/12 23:53:26 scheduler.go:667: Task taurusjob-web-prod-1436741598-c9cc085d-28e8-11e5-a327-600308a80bb2 is in state TASK_RUNNING
2015/07/12 23:53:26 scheduler.go:667: Task taurusjob-web-prod-1436741598-c9cbf28d-28e8-11e5-a327-600308a80bb2 is in state TASK_RUNNING
2015/07/12 23:53:26 scheduler.go:667: Task taurusjob-web-prod-1436741598-c9cc10ea-28e8-11e5-a327-600308a80bb2 is in state TASK_RUNNING
```

Check if the containers are running:
```
$ docker ps
CONTAINER ID        IMAGE               COMMAND                CREATED             STATUS              PORTS               NAMES
07bf0188afc6        nginx:latest        "nginx -g 'daemon of   8 seconds ago       Up 6 seconds        443/tcp, 80/tcp     mesos-466f9aa8-32e1-4148-9e42-c0716f8c594a
4e246cfab0f7        nginx:latest        "nginx -g 'daemon of   8 seconds ago       Up 6 seconds        443/tcp, 80/tcp     mesos-78df44ea-2ca6-448c-9dbf-9cf88081b8fc
af6fefad032c        nginx:latest        "nginx -g 'daemon of   8 seconds ago       Up 6 seconds        443/tcp, 80/tcp     mesos-a689eec3-6d63-44fb-818a-64c427b2b88a
98b0902a8aae        nginx:latest        "nginx -g 'daemon of   9 seconds ago       Up 8 seconds        443/tcp, 80/tcp     mesos-f8534b44-4c88-43b9-aced-21bee4c91f8a

```

Stop the job:
```
$ curl -i -X DELETE 127.0.0.1:8080/job/taurusjob
HTTP/1.1 200 OK
Content-Type: application/json; charset=UTF-8
Date: Sun, 12 Jul 2015 22:54:39 GMT
Content-Length: 0
```

Taurus logs:
```
2015/07/12 23:54:39 server.go:43: DELETE	/job/taurusjob
2015/07/12 23:54:39 api.go:131: Stopping Job taurusjob
2015/07/12 23:54:39 api.go:148: Killing Job taurusjob
2015/07/12 23:54:39 scheduler.go:258: Queueing task taurusjob-web-prod-1436741598-c9cc10ea-28e8-11e5-a327-600308a80bb2 to Doomed queue
2015/07/12 23:54:39 scheduler.go:258: Queueing task taurusjob-web-prod-1436741598-c9cc0d1c-28e8-11e5-a327-600308a80bb2 to Doomed queue
2015/07/12 23:54:39 scheduler.go:258: Queueing task taurusjob-web-prod-1436741598-c9cc085d-28e8-11e5-a327-600308a80bb2 to Doomed queue
2015/07/12 23:54:39 scheduler.go:258: Queueing task taurusjob-web-prod-1436741598-c9cbf28d-28e8-11e5-a327-600308a80bb2 to Doomed queue
...
...
2015/07/12 23:54:42 scheduler.go:667: Task taurusjob-web-prod-1436741598-c9cc0d1c-28e8-11e5-a327-600308a80bb2 is in state TASK_KILLED
2015/07/12 23:54:42 scheduler.go:667: Task taurusjob-web-prod-1436741598-c9cbf28d-28e8-11e5-a327-600308a80bb2 is in state TASK_KILLED
2015/07/12 23:54:42 scheduler.go:667: Task taurusjob-web-prod-1436741598-c9cc10ea-28e8-11e5-a327-600308a80bb2 is in state TASK_KILLED
2015/07/12 23:54:42 scheduler.go:667: Task taurusjob-web-prod-1436741598-c9cc085d-28e8-11e5-a327-600308a80bb2 is in state TASK_KILLED
```

Check if the containers are running:
```
$ docker ps
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
```

# TODO
**E_TOO_MUCH**
- redesign a lot of things (I might spin another project from this)
- implement re-registration and error recovery
- implement etcd/consul/zk storage driver support
- implement more scalable solution using something like graft by Apcera 

AND THE LIST GOES ON AND ON AND ON

package etcdummy

import (
	"net"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	ErrNotImplemented = status.Errorf(codes.Unimplemented, "not implemented")
	ErrInvalidLease   = status.Errorf(codes.NotFound, "invalid lease")
)

type Lease struct {
	ID         int64
	GrantedTTL int64
	Expires    time.Time
}

func (l Lease) TTL() int64 {
	ttl := int64(time.Until(l.Expires) / time.Second)
	if ttl < 0 {
		return 0
	}
	return ttl
}

type Watch struct {
	ID         int64
	Start, End string
	Filters    []etcdserverpb.WatchCreateRequest_FilterType

	stream etcdserverpb.Watch_WatchServer
}

func (w Watch) Match(key string) bool {
	if len(w.End) == 0 {
		return key == w.Start
	} else {
		fetchAllAfter := w.End == "\x00"
		fetchAll := w.Start == "\x00" && fetchAllAfter

		return fetchAll || key >= w.Start && (key < w.End || fetchAllAfter)
	}
}

type Server struct {
	sync.Mutex

	Addr net.Addr

	KV       map[string]mvccpb.KeyValue
	Revision int64

	Leases    map[int64]Lease
	LastLease int64

	Watches   map[int64]Watch
	LastWatch int64
}

func New() *Server {
	return &Server{
		KV:      map[string]mvccpb.KeyValue{},
		Leases:  map[int64]Lease{},
		Watches: map[int64]Watch{},
	}
}

func (s *Server) ListenAndServe(bind string) error {
	grpcServer := grpc.NewServer()
	etcdserverpb.RegisterLeaseServer(grpcServer, s)
	etcdserverpb.RegisterKVServer(grpcServer, s)
	etcdserverpb.RegisterWatchServer(grpcServer, s)

	lis, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}
	defer lis.Close()
	s.Addr = lis.Addr()

	if err := grpcServer.Serve(lis); err != nil {
		return err
	}
	return nil
}

func (s *Server) IterateKeysLocked(start, end string, f func(k string, kv mvccpb.KeyValue) error) error {
	if len(end) == 0 {
		kv, ok := s.KV[start]
		if !ok {
			return nil
		}
		if err := f(start, kv); err != nil {
			return err
		}
	} else {
		fetchAllAfter := end == "\x00"
		fetchAll := start == "\x00" && fetchAllAfter

		for k, v := range s.KV {
			if fetchAll || k >= start && (k < end || fetchAllAfter) {
				if err := f(k, v); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Range gets the keys in the range from the key-value store.
func (s *Server) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.checkExpiredLeasesLocked(); err != nil {
		return nil, err
	}

	var kvs []*mvccpb.KeyValue

	start := string(req.Key)
	end := string(req.RangeEnd)
	if err := s.IterateKeysLocked(start, end, func(k string, kv mvccpb.KeyValue) error {
		kvs = append(kvs, &kv)

		return nil
	}); err != nil {
		return nil, err
	}

	SortKVs(kvs)

	return &etcdserverpb.RangeResponse{
		Kvs:   kvs,
		Count: int64(len(kvs)),
	}, nil
}

// Put puts the given key into the key-value store.
// A put request increments the revision of the key-value store
// and generates one event in the event history.
func (s *Server) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.checkExpiredLeasesLocked(); err != nil {
		return nil, err
	}

	s.Revision++

	key := string(req.Key)
	prevKV, ok := s.KV[key]
	kv := prevKV
	kv.Key = req.Key
	if !req.IgnoreValue {
		kv.Value = req.Value
	}
	if !req.IgnoreLease {
		kv.Lease = req.Lease
	}
	kv.ModRevision = s.Revision
	if kv.CreateRevision == 0 {
		kv.CreateRevision = s.Revision
	}
	s.KV[key] = kv

	for _, w := range s.Watches {
		if w.Match(key) {
			if err := w.stream.Send(&etcdserverpb.WatchResponse{
				WatchId: w.ID,
				Events: []*mvccpb.Event{
					{
						Type:   mvccpb.PUT,
						Kv:     &kv,
						PrevKv: &prevKV,
					},
				},
			}); err != nil {
				return nil, err
			}
		}
	}

	resp := &etcdserverpb.PutResponse{}
	if ok {
		resp.PrevKv = &prevKV
	}
	return resp, nil
}

// DeleteRange deletes the given range from the key-value store.
// A delete request increments the revision of the key-value store
// and generates a delete event in the event history for every deleted key.
func (s *Server) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.checkExpiredLeasesLocked(); err != nil {
		return nil, err
	}

	s.Revision++

	var prevKvs []*mvccpb.KeyValue

	start := string(req.Key)
	end := string(req.RangeEnd)
	if err := s.IterateKeysLocked(start, end, func(k string, kv mvccpb.KeyValue) error {
		prevKvs = append(prevKvs, &kv)

		for _, w := range s.Watches {
			if w.Match(k) {
				if err := w.stream.Send(&etcdserverpb.WatchResponse{
					WatchId: w.ID,
					Events: []*mvccpb.Event{
						{
							Type:   mvccpb.DELETE,
							PrevKv: &kv,
						},
					},
				}); err != nil {
					return err
				}
			}
		}

		delete(s.KV, k)

		return nil
	}); err != nil {
		return nil, err
	}

	SortKVs(prevKvs)

	return &etcdserverpb.DeleteRangeResponse{
		PrevKvs: prevKvs,
		Deleted: int64(len(prevKvs)),
	}, nil
}

// Txn processes multiple requests in a single transaction.
// A txn request increments the revision of the key-value store
// and generates events with the same revision for every completed request.
// It is not allowed to modify the same key several times within one txn.
func (s *Server) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return nil, ErrNotImplemented
}

// Compact compacts the event history in the etcd key-value store. The key-value
// store should be periodically compacted or the event history will continue to grow
// indefinitely.
func (s *Server) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return nil, nil
}

func (s *Server) checkExpiredLeases() error {
	s.Lock()
	defer s.Unlock()

	return s.checkExpiredLeasesLocked()
}

func (s *Server) checkExpiredLeasesLocked() error {
	revoked := false

	for _, l := range s.Leases {
		if l.TTL() == 0 {
			if err := s.leaseRevokeLocked(l); err != nil {
				return err
			}
			revoked = true
		}
	}

	if revoked {
		s.Revision++
	}

	return nil
}

// LeaseGrant creates a lease which expires if the server does not receive a keepAlive
// within a given time to live period. All keys attached to the lease will be expired and
// deleted if the lease expires. Each expired key generates a delete event in the event history.
func (s *Server) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	s.Lock()
	defer s.Unlock()

	id := req.ID
	if id == 0 {
		s.LastLease++
		id = s.LastLease
	}

	lease := Lease{
		ID:         id,
		GrantedTTL: req.TTL,
		Expires:    time.Now().Add(time.Duration(req.TTL) * time.Second),
	}
	s.Leases[id] = lease

	return &etcdserverpb.LeaseGrantResponse{
		TTL: lease.TTL(),
		ID:  lease.ID,
	}, nil
}

func (s *Server) leaseRevokeLocked(l Lease) error {
	for k, kv := range s.KV {
		if kv.Lease == l.ID {
			for _, w := range s.Watches {
				if w.Match(k) {
					if err := w.stream.Send(&etcdserverpb.WatchResponse{
						WatchId: w.ID,
						Events: []*mvccpb.Event{
							{
								Type:   mvccpb.DELETE,
								PrevKv: &kv,
							},
						},
					}); err != nil {
						return err
					}
				}
			}

			delete(s.KV, k)
		}
	}
	delete(s.Leases, l.ID)

	return nil
}

// LeaseRevoke revokes a lease. All keys attached to the lease will expire and be deleted.
func (s *Server) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	s.Lock()
	defer s.Unlock()

	l, ok := s.Leases[req.ID]
	if !ok {
		return nil, ErrInvalidLease
	}

	s.Revision++
	s.leaseRevokeLocked(l)

	return &etcdserverpb.LeaseRevokeResponse{}, nil
}

// LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
// to the server and streaming keep alive responses from the server to the client.
func (s *Server) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		l, ok := s.Leases[req.ID]
		if !ok {
			return ErrInvalidLease
		}
		l.Expires = time.Now().Add(time.Duration(l.GrantedTTL) * time.Second)
		s.Leases[req.ID] = l

		if err := stream.Send(&etcdserverpb.LeaseKeepAliveResponse{
			ID:  req.ID,
			TTL: l.TTL(),
		}); err != nil {
			return err
		}
	}

	return nil
}

// LeaseTimeToLive retrieves lease information.
func (s *Server) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	s.Lock()
	defer s.Unlock()

	l, ok := s.Leases[req.ID]
	if !ok {
		return nil, ErrInvalidLease
	}

	keys := [][]byte{}

	for k, kv := range s.KV {
		if kv.Lease == req.ID {
			keys = append(keys, []byte(k))
		}
	}

	return &etcdserverpb.LeaseTimeToLiveResponse{
		ID:         l.ID,
		TTL:        l.TTL(),
		GrantedTTL: l.GrantedTTL,
		Keys:       keys,
	}, nil
}

// LeaseLeases lists all existing leases.
func (s *Server) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	s.Lock()
	defer s.Unlock()

	var leases []*etcdserverpb.LeaseStatus
	for _, l := range s.Leases {
		leases = append(leases, &etcdserverpb.LeaseStatus{
			ID: l.ID,
		})
	}

	return &etcdserverpb.LeaseLeasesResponse{
		Leases: leases,
	}, nil
}

// Watch watches for events happening or that have happened. Both input and output
// are streams; the input stream is for creating and canceling watchers and the output
// stream sends events. One watch RPC can watch on multiple key ranges, streaming events
// for several watches at once. The entire event history can be watched starting from the
// last compaction revision.
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		create := req.GetCreateRequest()
		if create != nil {
			s.Lock()
			s.LastWatch++
			s.Watches[s.LastWatch] = Watch{
				ID:      s.LastWatch,
				Start:   string(create.Key),
				End:     string(create.RangeEnd),
				Filters: create.Filters,
			}
			s.Unlock()
		}

		cancel := req.GetCancelRequest()
		if cancel != nil {
			s.Lock()
			delete(s.Watches, cancel.WatchId)
			s.Unlock()
			stream.Send(&etcdserverpb.WatchResponse{
				WatchId:      cancel.WatchId,
				Canceled:     true,
				CancelReason: "client canceled",
			})
		}
	}

	return ErrNotImplemented
}

func SortKVs(kvs []*mvccpb.KeyValue) {
	sort.Slice(kvs, func(i, j int) bool {
		return string(kvs[i].Key) < string(kvs[j].Key)
	})
}

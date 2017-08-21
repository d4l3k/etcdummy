package etcdummy

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"

	"golang.org/x/net/context"
)

var ctx = context.Background()

func TestPutRangeDelete(t *testing.T) {
	s := New()

	// Puts

	{
		putResp, err := s.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte("test/a"),
			Value: []byte("0"),
		})
		if err != nil {
			t.Fatal(err)
		}
		// Make sure first put response is correct.
		want := &etcdserverpb.PutResponse{}
		if !reflect.DeepEqual(putResp, want) {
			t.Errorf("server.Put(...) = %+v; not %+v", putResp, want)
		}
	}

	if _, err := s.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("test/b"),
		Value: []byte("0"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("test0"),
		Value: []byte("0"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("bar"),
		Value: []byte("0"),
	}); err != nil {
		t.Fatal(err)
	}

	{
		putResp, err := s.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte("bar"),
			Value: []byte("1"),
		})
		if err != nil {
			t.Fatal(err)
		}

		// Make sure overwrite response is correct.
		want := &etcdserverpb.PutResponse{
			PrevKv: &mvccpb.KeyValue{
				Key:            []byte("bar"),
				Value:          []byte("0"),
				CreateRevision: 4,
				ModRevision:    4,
			},
		}
		if !reflect.DeepEqual(putResp, want) {
			t.Errorf("server.Put(...) = %+v; not %+v", putResp, want)
		}
	}

	// Ranges

	{
		out, err := s.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte("bar"),
		})
		if err != nil {
			t.Fatal(err)
		}
		want := &etcdserverpb.RangeResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:            []byte("bar"),
					Value:          []byte("1"),
					CreateRevision: 4,
					ModRevision:    5,
				},
			},
			Count: 1,
		}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("server.Get(...) = %+v; not %+v", out, want)
		}
	}

	{
		out, err := s.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte("test"),
		})
		if err != nil {
			t.Fatal(err)
		}
		want := &etcdserverpb.RangeResponse{}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("server.Get(...) = %+v; not %+v", out, want)
		}
	}

	{
		out, err := s.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("test/"),
			RangeEnd: []byte("test0"),
		})
		if err != nil {
			t.Fatal(err)
		}
		want := &etcdserverpb.RangeResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:            []byte("test/a"),
					Value:          []byte("0"),
					CreateRevision: 1,
					ModRevision:    1,
				},
				{
					Key:            []byte("test/b"),
					Value:          []byte("0"),
					CreateRevision: 2,
					ModRevision:    2,
				},
			},
			Count: 2,
		}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("server.Get(...) = %+v; not %+v", out, want)
		}
	}

	// Deletes

	{
		out, err := s.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte("non-existent"),
		})
		if err != nil {
			t.Fatal(err)
		}
		want := &etcdserverpb.DeleteRangeResponse{}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("server.Get(...) = %+v; not %+v", out, want)
		}
	}

	{
		out, err := s.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte("bar"),
		})
		if err != nil {
			t.Fatal(err)
		}
		want := &etcdserverpb.DeleteRangeResponse{
			PrevKvs: []*mvccpb.KeyValue{
				{
					Key:            []byte("bar"),
					Value:          []byte("1"),
					CreateRevision: 4,
					ModRevision:    5,
				},
			},
			Deleted: 1,
		}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("server.Get(...) = %+v; not %+v", out, want)
		}
	}

	{
		out, err := s.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key:      []byte("test/"),
			RangeEnd: []byte("test0"),
		})
		if err != nil {
			t.Fatal(err)
		}
		want := &etcdserverpb.DeleteRangeResponse{
			PrevKvs: []*mvccpb.KeyValue{
				{
					Key:            []byte("test/a"),
					Value:          []byte("0"),
					CreateRevision: 1,
					ModRevision:    1,
				},
				{
					Key:            []byte("test/b"),
					Value:          []byte("0"),
					CreateRevision: 2,
					ModRevision:    2,
				},
			},
			Deleted: 2,
		}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("server.Get(...) = %+v; not %+v", out, want)
		}
	}

	{
		out, err := s.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte{0},
			RangeEnd: []byte{0},
		})
		if err != nil {
			t.Fatal(err)
		}
		want := &etcdserverpb.RangeResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:            []byte("test0"),
					Value:          []byte("0"),
					CreateRevision: 3,
					ModRevision:    3,
				},
			},
			Count: 1,
		}
		if !reflect.DeepEqual(out, want) {
			t.Errorf("server.Get(...) = %+v; not %+v", out, want)
		}
	}
}

func TestETCDClient(t *testing.T) {
	s := New()

	go func() {
		defer s.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		addr := s.Addr().(*net.TCPAddr)
		etcd, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{fmt.Sprintf("localhost:%d", addr.Port)},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer etcd.Close()

		session, err := concurrency.NewSession(etcd, concurrency.WithContext(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := etcd.Put(ctx, "a", "0"); err != nil {
			t.Fatal(err)
		}
		if _, err := etcd.Put(ctx, "b", "0", clientv3.WithLease(session.Lease())); err != nil {
			t.Fatal(err)
		}
		session.Close()
		out, err := etcd.Get(ctx, "", clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		want := clientv3.GetResponse(etcdserverpb.RangeResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:            []byte("a"),
					Value:          []byte("0"),
					CreateRevision: 1,
					ModRevision:    1,
				},
			},
			Count: 1,
		})
		if !reflect.DeepEqual(out, &want) {
			t.Errorf("etcd.Get(...) = %+v; not %+v", out, &want)
		}
	}()

	s.ListenAndServe(":0")
}

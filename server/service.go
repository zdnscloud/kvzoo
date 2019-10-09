package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/ptypes/empty"

	"github.com/zdnscloud/kvzoo"
	"github.com/zdnscloud/kvzoo/backend"
	pb "github.com/zdnscloud/kvzoo/proto"
)

const MaxOpenTxCount = 1000

type KVService struct {
	db       kvzoo.DB
	nextTxId int64

	openedTables map[string]kvzoo.Table
	tableLock    sync.RWMutex

	openedTxs map[int64]kvzoo.Transaction
	txLock    sync.RWMutex
}

func newKVService(dbPath string) (*KVService, error) {
	db, err := backend.New(dbPath)
	if err != nil {
		return nil, err
	}

	return &KVService{
		db:           db,
		nextTxId:     0,
		openedTables: make(map[string]kvzoo.Table),
		openedTxs:    make(map[int64]kvzoo.Transaction),
	}, nil
}

func (s *KVService) CreateOrGetTable(ctx context.Context, in *pb.CreateOrGetTableRequest) (*empty.Empty, error) {
	s.tableLock.Lock()
	defer s.tableLock.Unlock()

	if _, ok := s.openedTables[in.Name]; ok == false {
		table, err := s.db.CreateOrGetTable(in.Name)
		if err != nil {
			return nil, err
		}
		s.openedTables[in.Name] = table
	}

	return &empty.Empty{}, nil
}

func (s *KVService) DeleteTable(ctx context.Context, in *pb.DeleteTableRequest) (*empty.Empty, error) {
	s.tableLock.Lock()
	delete(s.openedTables, in.Name)
	s.tableLock.Unlock()

	if err := s.db.DeleteTable(in.Name); err != nil {
		return nil, err
	} else {
		return &empty.Empty{}, nil
	}
}

func (s *KVService) BeginTransaction(ctx context.Context, in *pb.BeginTransactionRequest) (*pb.BeginTransactionReply, error) {
	s.tableLock.RLock()
	defer s.tableLock.RUnlock()

	table, ok := s.openedTables[in.TableName]
	if ok == false {
		return nil, fmt.Errorf("table %s doesn't exists", in.TableName)
	}

	s.txLock.Lock()
	defer s.txLock.Unlock()
	if len(s.openedTxs) > MaxOpenTxCount {
		return nil, fmt.Errorf("too many transactions are opened")
	}

	tx, err := table.Begin()
	if err != nil {
		return nil, err
	}

	id := atomic.AddInt64(&s.nextTxId, 1)
	s.openedTxs[id] = tx
	return &pb.BeginTransactionReply{
		TxId: id,
	}, nil
}

func (s *KVService) CommitTransaction(ctx context.Context, in *pb.CommitTransactionRequest) (*empty.Empty, error) {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	tx, ok := s.openedTxs[in.TxId]
	if ok == false {
		return nil, fmt.Errorf("invalid transaction id")
	}

	err := tx.Commit()
	delete(s.openedTxs, in.TxId)
	if err != nil {
		return nil, err
	} else {
		return &empty.Empty{}, nil
	}
}

func (s *KVService) RollbackTransaction(ctx context.Context, in *pb.RollbackTransactionRequest) (*empty.Empty, error) {
	s.txLock.Lock()
	defer s.txLock.Unlock()

	tx, ok := s.openedTxs[in.TxId]
	if ok == false {
		return nil, fmt.Errorf("invalid transaction id")
	}

	err := tx.Rollback()
	delete(s.openedTxs, in.TxId)
	if err != nil {
		return nil, err
	} else {
		return &empty.Empty{}, nil
	}
}

func (s *KVService) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	s.txLock.RLock()
	defer s.txLock.RUnlock()

	tx, ok := s.openedTxs[in.TxId]
	if ok == false {
		return nil, fmt.Errorf("invalid transaction id")
	}

	value, err := tx.Get(in.Key)
	if err != nil {
		return nil, err
	}

	return &pb.GetResponse{
		Value: value,
	}, nil
}

func (s *KVService) List(ctx context.Context, in *pb.ListRequest) (*pb.ListResponse, error) {
	s.txLock.RLock()
	defer s.txLock.RUnlock()

	tx, ok := s.openedTxs[in.TxId]
	if ok == false {
		return nil, fmt.Errorf("invalid transaction id")
	}

	values, err := tx.List()
	if err != nil {
		return nil, err
	}

	return &pb.ListResponse{
		Values: values,
	}, nil
}

func (s *KVService) Add(ctx context.Context, in *pb.AddRequest) (*empty.Empty, error) {
	s.txLock.RLock()
	defer s.txLock.RUnlock()

	tx, ok := s.openedTxs[in.TxId]
	if ok == false {
		return nil, fmt.Errorf("invalid transaction id")
	}

	if err := tx.Add(in.Key, in.Value); err != nil {
		return nil, err
	} else {
		return &empty.Empty{}, nil
	}
}

func (s *KVService) Delete(ctx context.Context, in *pb.DeleteRequest) (*empty.Empty, error) {
	s.txLock.RLock()
	defer s.txLock.RUnlock()

	tx, ok := s.openedTxs[in.TxId]
	if ok == false {
		return nil, fmt.Errorf("invalid transaction id")
	}

	if err := tx.Delete(in.Key); err != nil {
		return nil, err
	} else {
		return &empty.Empty{}, nil
	}
}

func (s *KVService) Update(ctx context.Context, in *pb.UpdateRequest) (*empty.Empty, error) {
	s.txLock.RLock()
	defer s.txLock.RUnlock()

	tx, ok := s.openedTxs[in.TxId]
	if ok == false {
		return nil, fmt.Errorf("invalid transaction id")
	}

	if err := tx.Update(in.Key, in.Value); err != nil {
		return nil, err
	} else {
		return &empty.Empty{}, nil
	}
}

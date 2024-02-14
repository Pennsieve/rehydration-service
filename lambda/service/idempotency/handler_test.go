package idempotency

import (
	"context"
	"errors"
	"github.com/pennsieve/rehydration-service/service/models"
	"github.com/pennsieve/rehydration-service/service/request"
	"github.com/pennsieve/rehydration-service/shared/idempotency"
	"github.com/pennsieve/rehydration-service/shared/logging"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"log/slog"
	"testing"
)

type handlerTest struct {
	dataset models.Dataset
	user    models.User
	store   *MockStore
	ecs     *MockECSHandler
	handler Handler
}

func newHandlerTest(dataset models.Dataset, user models.User) *handlerTest {
	store := new(MockStore)
	ecs := new(MockECSHandler)
	handler := Handler{
		store: store,
		request: &request.RehydrationRequest{
			Dataset: dataset,
			User:    user,
			Logger:  logging.Default,
		},
		ecsHandler: ecs,
	}
	return &handlerTest{
		dataset: dataset,
		user:    user,
		store:   store,
		ecs:     ecs,
		handler: handler,
	}
}

func (h *handlerTest) assertMockAssertions(t *testing.T) {
	h.store.AssertExpectations(t)
	h.ecs.AssertExpectations(t)
}

func TestHandler_Handle(t *testing.T) {
	dataset := models.Dataset{ID: 4321, VersionID: 3}
	user := models.User{Name: "First Last", Email: "last@example.com"}
	test := newHandlerTest(dataset, user)

	expectedTaskARN := "arn:aws:ecs:test:test:test"
	test.store.OnSaveInProgressSucceed(dataset.ID, dataset.VersionID).Once()
	test.ecs.OnHandleReturn(dataset, expectedTaskARN).Once()

	resp, err := test.handler.Handle(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.RehydrationLocation)
	require.Equal(t, expectedTaskARN, resp.TaskARN)
	test.assertMockAssertions(t)
}

func TestHandler_Handle_NoRetry(t *testing.T) {
	dataset := models.Dataset{ID: 4321, VersionID: 3}
	user := models.User{Name: "First Last", Email: "last@example.com"}
	test := newHandlerTest(dataset, user)

	expectedError := errors.New("unexpected error")
	test.store.OnSaveInProgressError(dataset.ID, dataset.VersionID, expectedError).Once()

	_, err := test.handler.Handle(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, expectedError)
	test.assertMockAssertions(t)
}

func TestHandler_Handle_RetryOnce(t *testing.T) {
	dataset := models.Dataset{ID: 4321, VersionID: 3}
	user := models.User{Name: "First Last", Email: "last@example.com"}
	test := newHandlerTest(dataset, user)

	expectedTaskARN := "arn:aws:ecs:test:test:test:test"

	// Fist SaveInProgress returns an error indicating that a record already exists, but does not include info about it
	alreadyExistsError := idempotency.RecordAlreadyExistsError{}
	test.store.OnSaveInProgressError(dataset.ID, dataset.VersionID, alreadyExistsError).Once()

	// So the code attempts to look up the supposedly existing record, but gets nil. This results in
	// an inconsistent state error indicating that the record must have been deleted between SaveInProgress and GetRecord
	// This should cause a retry
	test.store.OnGetRecordReturn(idempotency.RecordID(dataset.ID, dataset.VersionID), nil).Once()

	// On the retry SaveInProgress now returns success to indicate that a new record was created
	test.store.OnSaveInProgressSucceed(dataset.ID, dataset.VersionID).Once()

	test.ecs.OnHandleReturn(dataset, expectedTaskARN).Once()

	resp, err := test.handler.Handle(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedTaskARN, resp.TaskARN)
	require.Empty(t, resp.RehydrationLocation)
	test.assertMockAssertions(t)
}

type MockStore struct {
	mock.Mock
}

func (m *MockStore) SaveInProgress(ctx context.Context, datasetID, datasetVersionID int) error {
	args := m.Called(ctx, datasetID, datasetVersionID)
	return args.Error(0)
}

func (m *MockStore) OnSaveInProgressSucceed(datasetID, datasetVersionID int) *mock.Call {
	return m.On("SaveInProgress", mock.Anything, datasetID, datasetVersionID).Return(nil)
}

func (m *MockStore) OnSaveInProgressError(datasetID, datasetVersionID int, err error) *mock.Call {
	return m.On("SaveInProgress", mock.Anything, datasetID, datasetVersionID).Return(err)
}

func (m *MockStore) GetRecord(ctx context.Context, recordID string) (*idempotency.Record, error) {
	args := m.Called(ctx, recordID)
	return args.Get(0).(*idempotency.Record), args.Error(1)
}

func (m *MockStore) OnGetRecordReturn(recordID string, ret *idempotency.Record) *mock.Call {
	return m.On("GetRecord", mock.Anything, recordID).Return(ret, nil)
}

func (m *MockStore) OnGetRecordError(recordID string, err error) *mock.Call {
	return m.On("GetRecord", mock.Anything, recordID).Return(nil, err)
}

func (m *MockStore) PutRecord(ctx context.Context, record idempotency.Record) error {
	args := m.Called(ctx, record)
	return args.Error(0)
}

func (m *MockStore) OnPutRecordSucceed(record idempotency.Record) *mock.Call {
	return m.On("PutRecord", mock.Anything, record).Return(nil)
}

func (m *MockStore) OnPutRecordError(record idempotency.Record, err error) *mock.Call {
	return m.On("PutRecord", mock.Anything, record).Return(err)
}

func (m *MockStore) UpdateRecord(ctx context.Context, record idempotency.Record) error {
	args := m.Called(ctx, record)
	return args.Error(0)
}

func (m *MockStore) OnUpdateRecordSucceed(record idempotency.Record) *mock.Call {
	return m.On("UpdateRecord", mock.Anything, record).Return(nil)
}

func (m *MockStore) OnUpdateRecordError(record idempotency.Record, err error) *mock.Call {
	return m.On("UpdateRecord", mock.Anything, record).Return(err)
}

func (m *MockStore) SetTaskARN(ctx context.Context, recordID string, taskARN string) error {
	args := m.Called(ctx, recordID, taskARN)
	return args.Error(0)
}

func (m *MockStore) OnSetTaskARNSucceed(recordID string, taskARN string) *mock.Call {
	return m.On("SetTaskARN", mock.Anything, recordID, taskARN).Return(nil)
}

func (m *MockStore) OnSetTaskARNError(recordID string, taskARN string, err error) *mock.Call {
	return m.On("SetTaskARN", mock.Anything, recordID, taskARN).Return(err)
}

func (m *MockStore) DeleteRecord(ctx context.Context, recordID string) error {
	args := m.Called(ctx, recordID)
	return args.Error(0)
}

func (m *MockStore) OnDeleteRecordSucceed(recordID string) *mock.Call {
	return m.On("DeleteRecord", mock.Anything, recordID).Return(nil)
}

func (m *MockStore) OnDeleteRecordError(recordID string, err error) *mock.Call {
	return m.On("DeleteRecord", mock.Anything, recordID).Return(err)
}

type MockECSHandler struct {
	mock.Mock
}

func (m *MockECSHandler) Handle(ctx context.Context, dataset models.Dataset, logger *slog.Logger) (string, error) {
	args := m.Called(ctx, dataset, logger)
	return args.String(0), args.Error(1)
}

func (m *MockECSHandler) OnHandleReturn(dataset models.Dataset, ret string) *mock.Call {
	return m.On("Handle", mock.Anything, dataset, mock.Anything).Return(ret, nil)
}

func (m *MockECSHandler) OnHandleError(dataset models.Dataset, err error) *mock.Call {
	return m.On("Handle", mock.Anything, dataset, mock.Anything).Return("", err)
}

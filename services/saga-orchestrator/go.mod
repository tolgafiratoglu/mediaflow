module github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator

go 1.25.0

require (
	github.com/google/uuid v1.6.0
	github.com/segmentio/kafka-go v0.4.47
	github.com/tolgafiratoglu/mediaflow/proto v0.0.0
	google.golang.org/grpc v1.68.0
	google.golang.org/protobuf v1.36.0
	gorm.io/driver/postgres v1.5.11
	gorm.io/gorm v1.25.12
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.7.5 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/net v0.29.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
)

replace github.com/tolgafiratoglu/mediaflow/proto => ../../proto

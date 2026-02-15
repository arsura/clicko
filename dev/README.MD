# clickhouse-migrator

ตัว CLI สำหรับจัดการ database migration บน ClickHouse เขียนใหม่ตั้งแต่ต้น (inspired by Goose) เพื่อรองรับ feature ที่ Goose ทำไม่ได้ เช่น cluster mode, custom engine, insert quorum

## Project Structure

```
clickhouse-migrator/
├── cmd/clickhouse-migrator/
│   ├── main.go                        # CLI entry point
│   └── test/cli/
│       ├── cli_test.go                # Integration test (ต่อ ClickHouse จริง)
│       └── testdata/migrations/       # Migration files สำหรับ integration test
├── pkg/migrator/
│   ├── types.go                       # Migration, MigrationSource structs
│   ├── loader.go                      # อ่าน .sql จาก directory
│   ├── loader_test.go                 # Unit test (testify suite)
│   ├── store.go                       # อ่าน/เขียน migration state ใน ClickHouse
│   ├── migrator.go                    # Logic หลัก (up, down, status)
│   └── testdata/loader/               # Test fixtures สำหรับ loader
├── dev/
│   ├── README.MD                      # ไฟล์นี้
│   └── cluster/
│       ├── docker-compose.yaml        # ClickHouse cluster 2 shard x 2 replica
│       ├── clickhouse/                # Config แต่ละ node
│       └── keeper/                    # ClickHouse Keeper config
├── Makefile                           # คำสั่ง make สำหรับจัดการ cluster
└── go.mod
```

## Installation

```bash
go install github.com/siwakorn-r/clickhouse-migrator/cmd/clickhouse-migrator@latest
```

หรือ build เอง:

```bash
go build -o clickhouse-migrator ./cmd/clickhouse-migrator
```

## CLI Usage

```
clickhouse-migrator [OPTIONS] COMMAND
```

### Commands

| Command    | คำอธิบาย                                            |
|------------|-----------------------------------------------------|
| `up`       | Apply migration ที่ยังไม่ได้ apply ทั้งหมด             |
| `up-to`    | Apply ถึง version ที่กำหนด                            |
| `down`     | Rollback migration ล่าสุด 1 ตัว                      |
| `down-to`  | Rollback ย้อนลงไปถึง version ที่กำหนด (ไม่รวมตัวนั้น)  |

### Options (Flags)

| Flag               | Default              | คำอธิบาย                                                              |
|--------------------|----------------------|-----------------------------------------------------------------------|
| `--dsn`            | *(required)*         | ClickHouse DSN เช่น `clickhouse://user:pass@host:9000/db`             |
| `--dir`            | `migrations`         | Directory ที่เก็บไฟล์ migration                                        |
| `--table`          | `migration_versions` | ชื่อ table สำหรับ track migration state                                |
| `--cluster`        | *(empty)*            | ชื่อ cluster (ใส่แล้วจะเปิด `ON CLUSTER` mode)                        |
| `--engine`         | *(auto)*             | Custom table engine สำหรับ tracking table (override default)           |
| `--insert-quorum`  | *(empty)*            | Insert quorum สำหรับ cluster writes (ตัวเลข หรือ `auto`)              |

### ตัวอย่างการใช้งาน

**Standalone (ไม่มี cluster):**

```bash
clickhouse-migrator \
  --dsn "clickhouse://default:@localhost:9000/default" \
  --dir ./migrations \
  up
```

**Cluster mode:**

```bash
clickhouse-migrator \
  --dsn "clickhouse://default:@localhost:29000/default" \
  --dir ./migrations \
  --cluster "all-replicated" \
  --engine "ReplicatedMergeTree('/clickhouse/all-replicated/tables/all/{database}/{table}', '{replica}')" \
  --insert-quorum 4 \
  up
```

## Migration File Convention

ไฟล์ migration ต้องตั้งชื่อตาม format:

```
<version>_<description>.<up|down>.sql
```

ตัวอย่าง:

```
00001_create_users.up.sql
00001_create_users.down.sql
00002_create_orders.up.sql
00002_create_orders.down.sql
```

### กฎ (Strict Validation)

- ทุกไฟล์ `.sql` ต้องตรงตาม naming convention ถ้าผิดแม้แต่ 1 ไฟล์ โปรแกรมจะ error ทันที
- ทุก version **ต้องมี** `.up.sql` (`.down.sql` optional)
- ถ้ามีทั้ง up และ down สำหรับ version เดียวกัน description ต้องตรงกัน
- Subdirectory จะถูกข้าม (ไม่อ่าน)
- ไฟล์ที่ไม่ใช่ `.sql` จะถูกข้าม
- ถ้า directory ว่างเปล่า ไม่ error (ได้ empty list)
- `down` command จะ error ถ้า migration ไม่มี `.down.sql`

## Migration Tracking Table

Schema ของ table ที่ใช้ track ว่า migration ไหน apply แล้ว:

```sql
CREATE TABLE migration_versions (
    version      UInt64,
    description  String,
    applied_at   DateTime DEFAULT now(),
    checksum     String
) ENGINE = MergeTree()  -- หรือ ReplicatedMergeTree ถ้าเป็น cluster mode
ORDER BY version
```

- `checksum` — สำรองไว้สำหรับ feature ในอนาคต (validate ว่า migration file ไม่ถูกแก้หลัง apply)

### Engine Selection (auto)

ถ้าไม่ได้ใส่ `--engine`:
- **ไม่มี `--cluster`** → ใช้ `MergeTree()`
- **มี `--cluster`** → ใช้ `ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/{database}/{table}', '{replica}')`

ถ้าใส่ `--engine` มา จะ override ทั้งหมด

## Local Development

### เริ่ม ClickHouse Cluster

จาก root ของ project:

```bash
make cluster-up       # Start cluster (2 shard x 2 replica + 1 keeper)
make cluster-down     # Stop & remove volumes
make cluster-restart  # Down แล้ว Up ใหม่
make cluster-logs     # ดู logs
make cluster-status   # ดูสถานะ containers
```

### Cluster Topology

| Container | Shard | Replica | TCP Port | HTTP Port |
|-----------|-------|---------|----------|-----------|
| ch-1-1    | 1     | 1       | 29000    | 28123     |
| ch-1-2    | 1     | 2       | 29001    | 28124     |
| ch-2-1    | 2     | 1       | 29002    | 28125     |
| ch-2-2    | 2     | 2       | 29003    | 28126     |

Cluster name: `all-replicated`

### ทดสอบ Connection

```bash
clickhouse-client --port 29000 -q "SELECT 1"
```

## Running Tests

### Unit Tests

```bash
go test ./pkg/migrator/ -v
```

ใช้ testify suite + testdata fixtures ไม่ต้องต่อ ClickHouse

### Integration Tests

ต้อง start cluster ก่อน:

```bash
make cluster-up
go test ./cmd/clickhouse-migrator/test/cli/ -v
```

ถ้า ClickHouse ไม่ได้รัน test จะ skip อัตโนมัติ (ไม่ fail)

## Architecture Notes

### Key Design Decisions

- **SQL-only** — รองรับแค่ `.sql` migration ยังไม่มี Go migration
- **Dependency Injection** — `Migrator` รับ `Loader` และ `Store` เป็น interface เข้ามา ทำให้ test ง่าย
- **Strict Validation** — ผิด convention = error ทันที ไม่มี silent skip
- **SQL Injection Prevention** — validate `TableName`, `Cluster`, `InsertQuorum` ก่อนใส่ใน SQL
- **Cluster-aware** — รองรับ `ON CLUSTER`, `ReplicatedMergeTree`, `insert_quorum`, `mutations_sync`
- **ClickHouse Settings via Context** — ใช้ `clickhouse.WithSettings()` แทนการเขียน `SETTINGS` ใน SQL ตรง ๆ เพราะ native driver ไม่รองรับ inline SETTINGS ใน INSERT

### Known Limitations

- ClickHouse ไม่มี transaction → ถ้า SQL สำเร็จแต่ store.Add/Remove fail จะเกิด inconsistent state
- แผนในอนาคต: retry, force-add/force-remove command, validate command

---

## Story

| Story | รายละเอียด |
|-------|-----------|
| CLI | ทำให้รันผ่าน Command Line ได้ |
| Custom Engine | สามารถใส่ Custom Engine ผ่าน CLI ได้ รวมถึง Zoo Path |
| Up, UpTo, Down, DownTo, Reset, Status, Version, Validate | Command ที่ใช้งานได้ |
| Go Integration | Go เรียกใช้งานผ่านโค้ดได้ |
| Easy Go Interface | Go เรียกใช้ได้ง่าย ๆ เขียน chmigrator.CreateTable() |

### สถานะปัจจุบัน (ทำแล้ว)

- [x] CLI: `up`, `up-to`, `down`, `down-to`
- [x] Custom Engine ผ่าน `--engine` flag
- [x] Cluster mode (`--cluster`, `--insert-quorum`)
- [x] Strict migration file validation
- [x] Unit tests (loader) + Integration tests (CLI + ClickHouse cluster)
- [x] SQL injection prevention
- [ ] Reset, Status (status มี logic แล้วแต่ยังไม่ต่อ CLI), Version, Validate
- [ ] Go Integration
- [ ] Easy Go Interface

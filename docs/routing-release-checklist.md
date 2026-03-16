# 라우팅 해제 작업 검증 체크리스트

목표: 대시보드에서 라우팅 해제 버튼 클릭 시 아래 3가지를 **동시에** 검증
- routing_registry: RELEASED/INACTIVE 반영
- ingestion_slot_registry: is_active=false, deactivated_at 반영
- 라우팅 엔진 테이블: 대상 객체 테이블 DROP 반영

## 1) 해제 실행
- ETL 대시보드에서 해제하려는 객체의 **해제** 버튼 클릭
- `ingestionMode`가 `STREAMING` 또는 `CDC`인지 확인
- 해제 완료 응답/화면에서 메시지(`resultSummary`)의 `responseBody` 또는 메시지에 `tableDropped=true` 또는 `tableDropped=false`가 포함되는지 확인

## 2) DB: routing_registry 확인
```sql
SELECT
  org_key,
  selected_object,
  routing_protocol,
  routing_status,
  source_status,
  released_at,
  last_error_message,
  updated_by,
  updated_at
FROM routing.routing_registry
WHERE org_key = :org_key
  AND selected_object = :selected_object
  AND routing_protocol = :routing_protocol
ORDER BY updated_at DESC
LIMIT 1;
```
- `routing_status` = `RELEASED`
- `source_status` = `INACTIVE`
- `released_at`가 해제 직후 시각인지

## 3) DB: ingestion_slot_registry 확인
```sql
SELECT
  org_key,
  selected_object,
  routing_protocol,
  is_active,
  deactivated_at,
  note,
  updated_at
FROM routing.ingestion_slot_registry
WHERE org_key = :org_key
  AND selected_object = :selected_object
  AND routing_protocol = :routing_protocol
ORDER BY updated_at DESC;
```
- `is_active` = `false`
- `deactivated_at`가 해제 직후 시각
- `note`에 "해지" 관련 메시지 존재

## 4) 라우팅 엔진 테이블 DROP 확인
- 메인 앱 경유 해제 시 `releaseObject`는 라우팅 엔진에 다음 endpoint를 호출합니다.
  - CDC: `POST /pubsub/drop`
  - Streaming: `POST /streaming/drop`
- 필요 시 DB에 직접 확인
  - MySQL: `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = :target_schema AND TABLE_NAME = :selected_object;`
  - 조회되지 않으면 DROP 성공
- 해제 응답 메시지 `responseBody`에 `tableDropped=true`가 들어오면 성공 판정

## 5) 대시보드 즉시 반영 확인
- 대시보드(`etl_dashboard`)에서 대상 객체가 목록에서 사라졌는지 확인
- 선택 org 기준(`?orgKey=...`) 조회 시에도 사라져야 함

## 6) 롤백/문제 발생 시 체크
- 위 2,3번 중 하나라도 반영되지 않으면 로그 확인:
  - `etl-dashboard` 해제 API 호출 로그
  - `etl-routing-engine`의 `/pubsub/drop` 또는 `/streaming/drop` 호출 로그
  - 라우팅 엔진 연결 및 `routing_engine.base-url` 설정값

## 참고
- 변경 포인트
  - `ETLServiceImpl.releaseObject`: release + table drop + 2개 레지스트리 동시 갱신
  - `RoutingDashboardRepository.deactivateSlotByObject`
  - `routingDashboardMapper.xml`의 `deactivateSlotByObject` 쿼리
  - `PubSubController`/`StreamingController`의 drop endpoint

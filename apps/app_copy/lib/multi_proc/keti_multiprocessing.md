# 코드 목적
- 멀티프로세스 생성
- 여러개 프로세서를 생성하면서, 메인 프로세스와 연결이 떨어짐
  

## 변수 확인
### apply 변수
- async_apply 실행을 위한 패러미터로 사용
- *args 로 호출됨
#### print (args) 실행결과 
```python
((<AutoProxy[Queue] object, 
   typeid 'Queue' at 0x7f4212b57250>, 
  <AutoProxy[Queue] object, 
   typeid 'Queue' at 0x7f4212b57610>, 
   None, 
   {'importname': 'user_prep', 'out_url': '125.140.110.217', 'mins': None, 'carid': '*', 'in_metric': 'HanuriTN_00', 'seconds': '86400', 'ip': '125.140.110.217', 'query_start': '2014/06/01-00:00:00', 'query_end': '2014/06/02-00:00:00', 'out_metric': '_spd_HTN_00_', 'out_port': '4242', 'dys': 1, 'content': 'spd', 'hrs': None, 'aggregator': 'none', 'works': 'num', 'port': '54242'}, 
   <function prep at 0x7f4212fc2b18>, 
   <Synchronized wrapper for c_int(0)>),)
```
- 요약해 보면
  1. 프로듀서 큐1
  2. 컨슈머 큐2
  3. 상태 큐3
  4. 메타 (쿼리정보)
  5. 프리프로세서 함수 주소
  6. 공유 value (사용하지 않음)
     - 프로듀서, 컨슈머 에서 서로의 상태를 알기 위한 공유변수 사용을 고려했으나
     - 프로듀서와 컨슈머 내부의 Limit을 정해서, Limit 을 넘어서는 경우는 종료하도록 범위 정의 

#### 작성한 테스트 코드 출력 (캡처)
``` print ('<pure>', funcargs, '<pure>')  ``` 출력 
```python
('<pure>', ((<AutoProxy[Queue] object, typeid 'Queue' at 0x7f6fb3b97250>, <AutoProxy[Queue] object, typeid 'Queue' at 0x7f6fb3b97610>, None, {'importname': 'user_prep', 'out_url': '125.140.110.217', 'mins': None, 'carid': '*', 'in_metric': 'HanuriTN_00', 'seconds': '86400', 'ip': '125.140.110.217', 'query_start': '2014/06/01-00:00:00', 'query_end': '2014/06/02-00:00:00', 'out_metric': '_spd_HTN_00_', 'out_port': '4242', 'dys': 1, 'content': 'spd', 'hrs': None, 'aggregator': 'none', 'works': 'num', 'port': '54242'}, <function prep at 0x7f6fb4002b18>, <Synchronized wrapper for c_int(0)>),), '<pure>')
```

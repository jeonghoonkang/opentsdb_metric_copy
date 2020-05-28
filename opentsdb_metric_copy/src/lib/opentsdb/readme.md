## * query_driver.py 수정사항

<pre>
190306 
- 실행 시 입력했던 정보와, Put 한 총 dps 개수, 실행 시간 등을 ../../lib/log/run_info_log.log 에 저장
- 연결 문제로 Put 하려고 했던 dps와 실제 Opentsdb에 저장된 dps의 개수가 다를 때가 있기 때문에 
  put 이후 'run_info_log.log' 파일을 열어 개수를 확인하는 것을 권장합니다.  
</pre>    

<pre>
190314 - query_nto_tsdb_v2 개발
         이전 버젼과의 차이점:
         날짜별로 Preprocessing을 나누는 것이 아니라, 
         차량별로 Preprocessing을 나눔.
    
         장점: 한번에 많은 날짜를 쿼리해도, 빠른 속도를 유지. 
         
         필요한 이유: non-exist 데이터는 날짜가 넘어가는 구간에 데이터가 많아서, 
                     그 구간이 포함되게 쿼리해야 데이터의 유실률을 낮출 수 있다.
                     긴 구간을 쿼리하면 전처리 속도가 현저히 떨어지게 된다.
                     따라서 긴 구간을 쿼리 하여도 전처리 속도를 높이기 위해 필요하다  
    
         실행조건: run.py에서 carid의 범위를 설정해줘야 함 (* lib_run.py 확인)
</pre> 

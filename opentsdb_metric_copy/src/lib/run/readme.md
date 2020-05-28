* lib_run.py 수정사항
<pre>
190305 
- shell 에서 'tag' = 'none' 도 가능하게 수정
- tags를 prep 내부에서 설정하게 함
  이유: 전처리 마다 원하는 tags가 다 다른데 매번 모든 tags를 쿼리하는것은 비효율
        따라서 전처리 내부에 tags를 설정해놓고 그 값을 가져다 쿼리
</pre>

<pre>
190314 
- set_carid 함수 생성: input에 맞는 carid 범위 return
- fname import 오류시 sys.path 
- 'ketidatetime' 함수로 시간 형식 맞춰주는 코드 제거 
</pre>



* lib_this_run.sh 수정사항







- spark.cores.max -> 1개 application에 할당되는 core수
- spark.executor.cores -> 1개 executor에 할당되는 core 수

-> spark.cores.max : 최대 = CPU core 수
-> spark.cores.max 12개 하고 spark.executor.cores = 4하면 3개 executor가 실행된다


- spark.driver.memory -> driver 가 사용하는 최대 메모리 
- spark.executor.memory -> 1개 executor 가 사용하는 최대 메모리

-> driver에 1g 주고 executor에 2g 줘도 실행됨 -> 드라이버랑 익스큐터는 별개

- spark.local.dir -> 스파크는 map output file이랑 rdd 를 디스크에 임시로 저장함 -> app 실행시 해당 폴더 생성 -> 경로를 지정해주는 옵션
 -> spark-defaults.conf 에서 지정 기본값은 /tmp

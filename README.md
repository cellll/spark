hi 

- spark.cores.max -> 1개 application에 할당되는 core수
- spark.executor.cores -> 1개 executor에 할당되는 core 수

-> spark.cores.max : 최대 = CPU core 수
-> spark.cores.max 12개 하고 spark.executor.cores = 4하면 3개 executor가 실행된다


- spark.driver.memory -> driver 가 먹는 메모리
- spark.executor.memory -> 1개 executor 가 먹는 메모리

-> driver에 1g 주고 executor에 2g 줘도 실행된다 -> 드라이버랑 익스큐터는 별개인게 밝혀짐

나머지는 뭐 별 볼게없음


- spark.local.dir -> 스파크는 map output file이랑 rdd 를 디스크에 임시로 저장함 -> 앱 돌리면 폴더가 생김 -> 요거 생기는것들 경로를 지정해주는거임 근데 저거는 spark-defaults.conf에다가 해야됨 기본값은 /tmp 얌

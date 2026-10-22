지구오락실 문제 자동저장 설정

1. GitHub 저장소에 index.html / server.js / package.json 업로드
2. Neon 등 PostgreSQL 서비스에서 데이터베이스 생성
3. Render 웹 서비스 > Environment > Add Environment Variable
   Key: DATABASE_URL
   Value: PostgreSQL 연결 문자열 (비밀 유지, GitHub에 업로드 금지)
4. Render 재배포 후 Logs에서 Question storage: PostgreSQL 확인
5. 관리자 문제 수정/저장 후 서버를 재시작하여 유지 여부 확인

DATABASE_URL이 없다면 서버의 questions.saved.json에 자동저장됩니다.
하지만 Render 무료 웹 서비스에서는 로컬 파일이 재배포/재시작 시 삭제될 수 있어
영구저장을 보장하지 않습니다. PostgreSQL 연결을 권장합니다.

저장: 객관식, 드로잉, 초성, OX 문제/정답/힌트/부가설명
저장 안 함: 점수, 참가자, 진행 중인 라운드

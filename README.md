# EGAT_API_forecast

docker build -t egat-forecast .
docker run -p 18901:18901 --env-file .env.dev egat-forecast

docker run --env-file .env.dev --network host egat-forecast
docker exec -t -i f5b5df75dc3d /bin/sh

f5b5df75dc3d
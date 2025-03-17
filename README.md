# EGAT_API_forecast

docker build -t egat-forecast .
docker run -p 18901:18901 --env-file .env.dev egat-forecast

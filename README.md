# EGAT_API_forecast

docker build -t egat-forecast .
docker build -t peamatchinginterface.azurecr.io/egat-forecast .
docker build -t peamatchinginterface.azurecr.io/egat-forecast:prd .
docker run -p 18901:18901 --env-file .env.dev egat-forecast
docker run -p 18901:18901 --env-file .env.dev peamatchinginterface.azurecr.io/egat-forecast
docker run -p 18901:18901 --env-file .env.dev peamatchinginterface.azurecr.io/egat-forecast:prd
docker push peamatchinginterface.azurecr.io/egat-forecast:prd

docker run --env-file .env.dev --network host egat-forecast
docker exec -t -i 35aeb5beb286 /bin/sh

f5b5df75dc3d
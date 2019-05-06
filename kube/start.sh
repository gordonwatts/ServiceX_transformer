echo "Adding x509 cert needed for data access"
kubectl delete secret -n servicex x509-secret
kubectl create secret -n servicex generic x509-secret --from-file=userkey=secrets/xcache.key.pem --from-file=usercert=secrets/xcache.crt.pem

REM echo "Adding conf"
REM kubectl delete secret -n servicex config
REM kubectl create secret -n servicex generic config --from-file=conf=../config/config.json


kubectl create -f transformer.yaml


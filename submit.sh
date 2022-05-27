#/bin/sh
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  ./program.py  \
  --output $1 

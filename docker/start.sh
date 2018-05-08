cd /data/srv
verb=start; for f in enabled/*; do
  app=${f#*/}; case $app in frontend) u=root ;; * ) u=_$app ;; esac; sh -c \
  "$PWD/current/config/$app/manage $verb 'I did read documentation'"
  if [ "$app" == "dbs" ] || [ "$app" == "dbsmigration" ]; then
    sh -c "$PWD/current/config/$app/manage setinstances 'I did read documentation'"
  fi
done

service ssh start

export IP_ADD=$IP_ADDRESS
echo $IP_ADD

bash ./this_run.sh

while true; 
  do echo "still live"; 
  sleep 600; 
done
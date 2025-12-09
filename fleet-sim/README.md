# Fleet Simulator

The repo deploys the fleet simulators. 

``` 
sites:
  - id: a
    namespace: site-a
    truckCount: 1
  - id: b
    namespace: site-b
    truckCount: 1
  - id: c
    namespace: site-c
    truckCount: 1


brokerHost: mqtt-lb.mqtt-broker.svc.cluster.local
brokerPort: 8883
repoURL: https://github.com/sumicsamy/mqtt.git
image: quay.io/suchugh/fleet-sim:0.0.23
platform: amq
```

Deploy {{truckCount}} sims for each {{site}}. Each truck produces data in the format:

```
return {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "region": self.region,
            "truck": self.truck_id,
            "loc": {"lat": self.lat, "lon": self.lon, "alt": alt},
            "spd": spd,
            "eng": {
                "rpm": rpm,
                "tmp": round(eng_tmp,1),
                "oil": round(random.uniform(3.0,4.5),1),
                "fuel": round(fuel,1),
            },
            "load": {
                "gross_t": round(gross,1),
                "pay_t": round(pay,1),
                "tray": round(tray,1),
            },
            "sys": {
                "cpu": sys_cpu,
                "mem": sys_mem,
                "tmp": round(sys_tmp,1),
                "lat": sys_lat,
                "sig": sys_sig,
            },
            "state": self.state,
        }
```

where state can be mapped to 

```
STATE_DRIVING_TO_CRUSHER = "driving_to_crusher"
STATE_UNLOADING = "unloading"
STATE_DRIVING_TO_LOADER = "driving_to_loader"
STATE_LOADING = "loading"
STATE_QUEUEING_AT_LOADER = "queueing_loader"
```

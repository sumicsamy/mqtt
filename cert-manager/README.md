###
Create CA & Certificate for broker along with trust manager to create trust store
```
Create self signed Cluster issuer as Root CA
            |
            |
            |
fleet-root-ca Cluster Issuer as Intermediate
            |
            |
            |
    Individual Certificates  
```

This assumes cert-manager operator is installed cluster wide

It installs:

1. Self signed Root CA
2. Intermediate fleet-root Cert
3. Certificate for Broker
4. Trust manager with trust bundle for fleet-root
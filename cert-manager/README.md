###
Create secret with Root CA certs for cluster issuer to use

```oc create secret tls root-ca --cert=amq-broker/ssl/root.crt --key=amq-broker/ssl/root.key -n cert-manager```

The same root ca will be used by site/trucks for client certs hence broker only needs to trust the root ca for this demo.
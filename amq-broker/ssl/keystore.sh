#!/usr/bin/env bash
# inputs: tls.crt, tls.key, ca.crt
PASSWORD='changeit'

# 1) Make a PKCS12 bundle from PEM
openssl pkcs12 -export \
  -in root.crt -inkey root.key \
  -out broker.p12 -name broker \
  -passout pass:${PASSWORD}

# 2) Convert PKCS12 -> JKS keystore (optional; PKCS12 works too)
keytool -importkeystore \
  -srckeystore broker.p12 -srcstoretype PKCS12 -srcstorepass ${PASSWORD} \
  -destkeystore broker.ks -deststoretype JKS -deststorepass ${PASSWORD}

# 3) Create truststore with your CA chain
keytool -import -noprompt \
  -keystore broker.ts -storepass ${PASSWORD} \
  -alias ca -file ca.crt
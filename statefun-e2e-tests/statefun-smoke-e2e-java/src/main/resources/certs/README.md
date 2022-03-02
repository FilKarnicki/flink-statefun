For instructions on how these cert/key files got created, see statefun-flink/statefun-flink-core/src/test/resources/certs/README.md

Plus,
```bash
keytool -import -alias bundle -trustcacerts -file a_ca.pem -keystore a_truststore.jks -storepass changeit -noprompt
openssl pkcs12 -export -name a_server -in a_server.crt -inkey a_server.key.p8 -out a_server.p12
```

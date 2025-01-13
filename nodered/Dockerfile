# nodered/Dockerfile
FROM nodered/node-red:latest


COPY . ./

RUN npm install node-red-contrib-coap
RUN npm install node-red-dashboard
RUN npm install node-red-node-mysql
RUN npm install node-red-contrib-influxdb
RUN npm install node-red-contrib-whatsapp-cmb
RUN npm install node-red-node-email
RUN npm install node-red-contrib-oauth2
RUN npm install @meowwolf/node-red-contrib-amqp
RUN npm install node-red-contrib-ui-led
RUN npm install node-red-contrib-mqtt-dynamictopic
RUN npm install node-red-contrib-email-dynamic





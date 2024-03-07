import { InfluxDB, Point } from '@influxdata/influxdb-client'
import mqtt from 'mqtt'
import 'dotenv/config'

const url = process.env.INFLUXDB_URL || ''
const token = process.env.INFLUXDB_TOKEN || ''
const org = process.env.INFLUXDB_ORG_ID || ''
const bucket = process.env.INFLUXDB_BUCKET || ''
const mqttUrl = process.env.MQTT_URL || ''

const influxDB = new InfluxDB({ url, token })
const client = mqtt.connect(mqttUrl)

client.on('connect', () => {
  console.log('MQTT Service Listening ...')
  client.subscribe('AI/v1/+/+/data', (err) => {
    if (err) console.error(err)
  });
});

client.on('message', (topic, message) => {
  const gateway = topic.split('/')[2]
  const device = topic.split('/')[3]
  const { ts } = JSON.parse(message.toString())

  const writeApi = influxDB.getWriteApi(org, bucket)
  const point = new Point('json_logs')
    .tag('device', device)
    .tag('gateway', gateway)
    .stringField('value', message.toString())
    .timestamp(new Date(ts * 1000));


  writeApi.writePoint(point)
  writeApi.close()
});
from __future__ import absolute_import

from carbon import log
from carbon.protocols import MetricReceiver
from carbon.client import CarbonClientProtocol, CarbonClientFactory


import kafka

# TODO: Implement a MetricReceiver(), see amqp/manhole for examples.


class CarbonKafkaClientProtocol(CarbonClientProtocol):

  def __init__(self):
      super(CarbonClientProtocol, self).__init__()
      self.producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')

  def _sendDatapointsNow(self, datapoints):
    print(datapoints)
    for metric, datapoint in datapoints:
      if isinstance(datapoint[1], float):
        value = ("%.10f" % datapoint[1]).rstrip('0').rstrip('.')
      else:
        value = "%d" % datapoint[1]
      # TODO: support protobuf / pickle / ..
      self.producer.send('TestTopic', key=metric, value="%s %d" % (value, datapoint[0]))


class CarbonKafkaClientFactory(CarbonClientFactory):
  plugin_name = "kafka"

  def clientProtocol(self):
      return CarbonKafkaClientProtocol()

  def startConnecting(self):
      self.started = True
      self.connector = None
      self.protocol = self.buildProtocol('fake')
      self.protocol.connectionMade()

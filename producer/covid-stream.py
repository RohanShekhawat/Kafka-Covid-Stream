import json, requests, os, logging
from confluent_kafka import Producer

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


def fail(err, msg):
    if err is not None:
        logger.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))


def producer_trigger(raw_data, context):
    state_stats_url = ('https://api.covid19india.org/data.json')
    district_stats_url = ('https://api.covid19india.org/v2/state_district_wise.json')
    bootstrap_servers = "localhost:9092"
    kafka_district_data_topic_name = "distict-data"
    kafka_state_data_topic_name = "state-data"
   

    conf = {'bootstrap.servers': bootstrap_servers}

    producer = Producer(conf, logger=logger)

    # import raw district data
    district_data = requests.get(district_stats_url).json()
    for data in district_data:
        state = data['state']
        district_data = data['districtData']
        for dd in district_data:
            district = dd['district']
            key = dict({'state': state, 'district': district})
            value = dict({'state': state, 'district': district, 'active': dd['active'], 'confirmed': dd['confirmed'],
                          'recovered': dd['recovered'], 'deceased': dd['deceased'],
                          'deltaConfirmed': dd['delta']['confirmed'],
                          'deltaRecovered': dd['delta']['recovered'], 'deltaDeceased': dd['delta']['deceased'],
                          'notes': dd['notes']
                          })
            try:
                producer.produce(topic=kafka_district_state_data_topic_name, value=json.dumps(value), key=json.dumps(key),
                                 on_delivery=fail)
            except BufferError:
                logger.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))
            producer.poll(0)
    logger.info('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()

    state_data = requests.get(state_stats_url).json()
    for k in state_data['statewise']:
        state = k['state']
        key = dict({'state': state})
        value = dict({'state': state, 'active': k['active'], 'confirmed': k['confirmed'],
                'recovered': k['recovered'], 'deceased': k['deaths'],
                'last_updated' : k['lastupdatedtime']
                })
        try:
            producer.produce(topic=kafka_current_data_topic_name, value=json.dumps(value), key=json.dumps(key),
                            on_delivery=fail)
        except BufferError:
            logger.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))
        producer.poll(0)
    logger.info('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()


if __name__ == '__main__':
    raw_data = dict()
    producer_trigger(raw_data, None)
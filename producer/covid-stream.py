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
    kafka_district_data_topic_name = "district-data"
    kafka_processed_data_topic_name = "processed-data"
   

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
                producer.produce(topic=kafka_district_data_topic_name, value=json.dumps(value), key=json.dumps(key),
                                 on_delivery=fail)
            except BufferError:
                logger.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))
            producer.poll(0)
    logger.info('%% Waiting for %d deliveries\n' % len(producer))
    producer.flush()

    district_data = requests.get(district_stats_url).json()
    for data in district_data:
        state = data['state']
        district_data = data['districtData']
        finalDict = {}
        for dd in district_data:
            district = dd['district']
            key = dict({'state': state, 'district': district})
            if(dd['active'] < 200):
                finalDict.update({ 'low_risk_zone' : dict({'state': state, 'district': district, 'active': dd['active'],'confirmed': dd['confirmed'],
                          'recovered': dd['recovered'], 'deceased': dd['deceased'],
                          'deltaConfirmed': dd['delta']['confirmed'],
                          'deltaRecovered': dd['delta']['recovered'], 'deltaDeceased': dd['delta']['deceased'],
                          'notes': dd['notes']})})
            elif(dd['active'] > 200 and dd['active'] < 800):
                finalDict.update({ 'moderate_risk_zone' : dict({'state': state, 'district': district, 'active': dd['active'] , 'confirmed': dd['confirmed'],
                          'recovered': dd['recovered'], 'deceased': dd['deceased'],
                          'deltaConfirmed': dd['delta']['confirmed'],
                          'deltaRecovered': dd['delta']['recovered'], 'deltaDeceased': dd['delta']['deceased'],
                          'notes': dd['notes']})})
            elif(dd['active'] > 800):
                finalDict.update({ 'high_risk_zone' : dict({'state': state, 'district': district, 'active': dd['active'] , 'confirmed': dd['confirmed'],
                          'recovered': dd['recovered'], 'deceased': dd['deceased'],
                          'deltaConfirmed': dd['delta']['confirmed'],
                          'deltaRecovered': dd['delta']['recovered'], 'deltaDeceased': dd['delta']['deceased'],
                          'notes': dd['notes']})})
            try:
                producer.produce(topic=kafka_processed_data_topic_name, value=json.dumps(finalDict), key=json.dumps(key),
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

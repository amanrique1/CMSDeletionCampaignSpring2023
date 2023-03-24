from rucio.client import Client
import pandas as pd
import multiprocessing as mp
import logging

logging.basicConfig(filename="deletion_campaign_deletion.log",filemode='a' ,level=logging.INFO, format='%(levelname)s %(asctime)s [%(name)s] %(filename)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')


def delete_rule(rule_id):

    client = clients[int(mp.current_process().name.split('-')[1])-1]
    try:
        updated = client.update_replication_rule(rule_id,{'locked':False})
        if updated:
            deleted = client.delete_replication_rule(rule_id,purge_replicas=False)
            if deleted:
                logging.info("Deleted rule %s" % rule_id)
            else:
                logging.error("Failed to delete rule %s" % rule_id)
        else:
            logging.error("Failed to unlock rule %s" % rule_id)
    except Exception as e:
        logging.error("Error deleting rule %s: %s" % (rule_id, e))

if __name__ == '__main__':
    clients = [Client() for _ in range(mp.cpu_count())]

    pool = mp.Pool(mp.cpu_count())

    df = pd.read_csv('deletion_rules_spring2023.csv')
    df = df.query('RSE_TYPE == "DISK"')
    rules = df['RULE_ID'].to_list()

    for rule_id in rules:
        pool.apply_async(delete_rule, args=(rule_id,))
    pool.close()
    pool.join()



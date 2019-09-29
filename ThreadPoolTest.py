import logging
import threading
import concurrent.futures as cf
import time
import random

def thread_function(name, wait_time):
    wait_time = random.randrange(5,15)
    logging.info('Thread {}: starting and waiting {}'.format(name, wait_time))
    time.sleep(wait_time)
    logging.info("Thread %s: finishing", name)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    with cf.ThreadPoolExecutor(max_workers=3) as executor:
    # with cf.ThreadPoolExecutor(max_workers=3) as executor:
        logging.info('Starting')
        executor.submit(thread_function, range(3))

    logging.info('Done')


    with concurrent.futures.ProcessPoolExecutor() as executor:
        for number, prime in zip(PRIMES, executor.map(is_prime, PRIMES)):
            print('%d is prime: %s' % (number, prime))

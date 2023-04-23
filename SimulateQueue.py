import heapq
import simpy
import numpy as np
import pandas as pd


def simulate_queue(arrival_rates: np.ndarray, service_rate: float, until: int = 5000) -> pd.DataFrame:
    """
    Simulates a single server non-preemptive priority queue.

    :param arrival_rates: array of arrival rates for each priority class
    :param service_rate: service rate of the server (must be greater than the sum of arrival rates to maintain steady state)
    :param until: the simulation will stop after n time units
    :return: DataFrame of simulation results
    """
    total_jobs = {}

    class PriorityQueue:
        """A single server non-preemptive priority queue object."""
        def __init__(self, env: simpy.Environment):
            self.env = env
            self.queue = []
            heapq.heapify(self.queue)
            self.busy = False

        def add(self, job, priority):
            """
            Adds jobs to the queue while pushing higher priority jobs to the front.

            :param job: item to be served
            :param priority: priority number
            """
            heapq.heappush(self.queue, (priority, job))

        def start(self, server):
            """
            If the queue is not empty the highest priority job is sent to service.

            :param server: server object
            """
            while True:
                if not self.queue:
                    self.busy = False
                    yield self.env.timeout(1)
                else:
                    self.busy = True
                    priority, job = heapq.heappop(self.queue)
                    self.env.process(server.serve(job))

    class Server:
        """A single server object that processes jobs given a mean service rate."""
        def __init__(self, env: simpy.Environment, svc_rate):
            self.env = env
            self.service_rate = svc_rate

        def serve(self, job):
            """
            Serves a single job.

            :param job: item to be served
            """
            service_time = np.random.exponential(scale=self.service_rate)
            yield self.env.timeout(service_time)
            job.exit_time = self.env.now
            total_jobs[job.id] = {**total_jobs[job.id], 'exit_time': job.exit_time}
            print(f"Job {job.__str__()} finished at time {job.exit_time}, priority {job.priority}")

    class Job:
        """A job object to be serviced by the system."""
        def __init__(self, env: simpy.Environment, job_id: int, priority: int):
            self.env = env
            self.id = job_id
            self.priority = priority
            self.enter_time = None
            self.exit_time = None

        def __str__(self):
            return f"{self.id}-{self.priority}"

        def __lt__(self, other):
            return self.priority < other.priority

        def start(self):
            """
            The job enters the queue.
            """
            self.enter_time = self.env.now
            total_jobs[self.id] = {'priority': self.priority, 'enter_time': self.enter_time}
            print(f"Job {self.__str__()} entered the system at time {self.enter_time}, priority {self.priority}")

    class SimQueue:
        """Simulates jobs entering the system and being served."""
        def __init__(self, arr_rates, svc_rate):
            self.env = simpy.Environment()
            self.arrival_rates = arr_rates
            self.server = Server(self.env, svc_rate)
            self.queue = PriorityQueue(self.env)
            self.job_id = 0

            # send jobs for each class to the queue based on their arrival rates
            for i in range(arr_rates.shape[0]):
                self.env.process(self.generate_jobs(i))
                self.env.process(self.process_jobs())

        def start(self, sim_time):
            """
            Starts the simulation.

            :param sim_time: maximum unit of time to run the simulation until
            """
            self.env.run(until=sim_time)

        def generate_jobs(self, class_id):
            """
            Adds jobs to the queue based on the class and its arrival rate.

            :param class_id: number representing the priority
            """
            while True:
                interarrival_time = np.random.poisson(self.arrival_rates[class_id])
                yield self.env.timeout(interarrival_time)
                self.job_id += 1
                job = Job(self.env, self.job_id, class_id)
                self.queue.add(job, class_id)
                job.start()

        def process_jobs(self):
            """
            When the queue is non-empty and the server is not busy, the next job gets processed.
            """
            while True:
                if not self.queue.busy and self.queue.queue:
                    self.env.process(self.queue.start(self.server))
                yield self.env.timeout(1)

    sim_env = SimQueue(arr_rates=arrival_rates, svc_rate=service_rate)
    sim_env.start(until)

    return pd.DataFrame(total_jobs).T

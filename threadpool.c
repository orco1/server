#include "threadpool.h"


threadpool* create_threadpool(int num_threads_in_pool)
{
	if (num_threads_in_pool <= 0 || num_threads_in_pool > MAXT_IN_POOL)
		return NULL;

	threadpool* pool = (threadpool*)malloc(sizeof(threadpool));
	pool->num_threads = num_threads_in_pool;	
	pool->qsize = 0;	        				
	
	pool->threads = (pthread_t*)malloc(pool->num_threads*sizeof(pthread_t)); 

	pool->qhead = pool->qtail = NULL;								
	pthread_mutex_init(&pool->qlock, NULL);		
	pthread_cond_init(&pool->q_not_empty, NULL);
	pthread_cond_init(&pool->q_empty, NULL);
	pool->shutdown = 0;                 
	pool->dont_accept = 0; 
	for (int i = 0; i < pool->num_threads; i++)
		pthread_create(&pool->threads[i], NULL, do_work, pool);
	return pool;
}

void dispatch(threadpool* pool, dispatch_fn dispatch_to_here, void *arg)
{
	pthread_mutex_lock(&pool->qlock);
	if(pool->dont_accept == 1)
	{
		pthread_mutex_unlock(&pool->qlock);
		return;
	}
	work_t* newJob = (work_t*)malloc(sizeof(work_t));
	newJob->routine = dispatch_to_here;
	newJob->arg = arg;
	newJob->next = NULL;

	if(!pool->qhead)
		pool->qhead = pool->qtail = newJob;
	
	else
	{
		pool->qtail->next = newJob;
		pool->qtail = pool->qtail->next;
	}

	pool->qsize++;
	pthread_mutex_unlock(&pool->qlock);
	pthread_cond_signal(&pool->q_not_empty);
}

/**
 * The work function of the thread
 * this function should:
 * 1. lock mutex
 * 2. if the queue is empty, wait
 * 3. take the first element from the queue (work_t)
 * 4. unlock mutex
 * 5. call the thread routine
 *
 */
void* do_work(void* p)
{
	threadpool* pool = (threadpool*)p;
	while(1)
	{
		if(pool->shutdown == 1)
			return NULL;

		pthread_mutex_lock(&pool->qlock);
		if(pool->qsize == 0)
		{
			pthread_cond_wait(&pool->q_not_empty, &pool->qlock);
			if(pool->shutdown == 1)
			{
				pthread_mutex_unlock(&pool->qlock);
				return NULL;
			}
		}
		work_t* temp = dequeueJob(pool);
		if(!temp)
		{
			pthread_mutex_unlock(&pool->qlock);
			continue;
		}

		pthread_mutex_unlock(&pool->qlock);
		temp->routine(temp->arg);
		free(temp);

		if(pool->qsize == 0 && pool->dont_accept == 1)
			pthread_cond_signal(&pool->q_empty);
	}
}


void destroy_threadpool(threadpool* destroyme)
{
	pthread_mutex_lock(&destroyme->qlock);
	destroyme->dont_accept = 1;
	if(destroyme->qsize > 0)
		pthread_cond_wait(&destroyme->q_empty, &destroyme->qlock);

	destroyme->shutdown = 1;
	pthread_mutex_unlock(&destroyme->qlock);
	pthread_cond_broadcast(&destroyme->q_not_empty);

	for (int i = 0; i < destroyme->num_threads; i++)
	{
		pthread_join(destroyme->threads[i], NULL);
	}

	free(destroyme->threads);
	pthread_mutex_destroy(&destroyme->qlock);
	pthread_cond_destroy(&destroyme->q_not_empty);
	pthread_cond_destroy(&destroyme->q_empty);
	free(destroyme);
}


work_t* dequeueJob(threadpool* pool)
{
	if(!pool->qhead)
		return NULL;

	work_t* job = pool->qhead;
	pool->qhead = pool->qhead->next;
	pool->qsize--;
	return job;
}

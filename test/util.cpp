#include <thread>

#include "numberkv.h"
#include "graph.h"

template <>
status_t numberkv_t<int>::batch_update(const string& src, const string& dst, propid_t pid /* = 0 */)
{
    edgeT_t<int> edge; 
    edge.src_id = g->get_sid(src.c_str());
    edge.dst_id = strtol(src.c_str(),0, 10);
    return batch_edge(edge);
}

template <>
status_t numberkv_t<uint64_t>::batch_update(const string& src, const string& dst, propid_t pid /* = 0 */)
{
    edgeT_t<uint64_t> edge; 
    edge.src_id = g->get_sid(src.c_str());
    edge.dst_id = strtol(src.c_str(),0, 10);
    return batch_edge(edge);
}

short CorePin(int coreID)
{
   int s, j;
   cpu_set_t cpuset;
   pthread_t thread;

   thread = pthread_self();

   /* Set affinity mask to include CPUs 0 to 7 */

   CPU_ZERO(&cpuset);
   CPU_SET(coreID, &cpuset);

   //for (j = 0; j < 8; j++) CPU_SET(j, &cpuset);

   s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0) {
       cout << "failed to set the core" << endl;
       //handle_error_en(s, "pthread_setaffinity_np");
   }

   /* Check the actual affinity mask assigned to the thread */
  /*
   s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0)
       handle_error_en(s, "pthread_getaffinity_np");

   printf("Set returned by pthread_getaffinity_np() contained:\n");
   for (j = 0; j < CPU_SETSIZE; j++)
       if (CPU_ISSET(j, &cpuset))
           printf("    CPU %d\n", j);
    */
   return 0;
}

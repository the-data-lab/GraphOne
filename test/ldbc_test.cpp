#include <set>
#include <list>
#include "all.h"
#include "csv_to_edge.h"
//#include "query.h"
//#include "iterative_analytics.h"

void schema_ldbc()
{
    g->cf_info  = new cfinfo_t*[64];
    g->p_info       = new pinfo_t[64];
    
    pinfo_t*    p_info    = g->p_info;
    cfinfo_t*   info      = 0;
    const char* longname  = 0;
    const char* shortname = 0;
    
    longname = "gtype";
    shortname = "gtype";
    info = new typekv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    ++p_info;
    
    longname = "comment_hasCreator_person";
    shortname = "comment_hasCreator_person";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0);
    info->flag2 = (1<<2);
    ++p_info;
    
    longname = "comment_hasTag_tag";
    shortname = "comment_hasTag_tag";
    info = new dgraph_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0);
    info->flag2 = (1<<5);
    ++p_info;
    
    longname = "comment_isLocatedIn_place";
    shortname = "comment_isLocatedIn_place";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0);
    info->flag2 = (1<<3);
    ++p_info;
    
    longname = "comment_replyOf_comment";
    shortname = "comment_replyOf_comment";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0);
    info->flag2 = (1<<0);
    ++p_info;
    
    longname = "comment_replyOf_post";
    shortname = "comment_replyof_post";
    //info = new many2one_t;
	info = new dgraph_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0);
    info->flag2 = (1<<4);
    ++p_info;
    
    longname = "post_hasCreator_person";
    shortname = "post_hasCreator_person";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<4);
    info->flag2 = (1<<2);
    ++p_info;
    
    longname = "post_hasTag_tag";
    shortname = "post_hasTag_tag";
    info = new dgraph_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<4);
    info->flag2 = (1<<5);
    ++p_info;
    
    longname = "post_isLocatedIn_place";
    shortname = "post_isLocatedIn_place";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<4);
    info->flag2 = (1<<3);
    ++p_info;
    
    longname = "forum_containerOf_post";
    shortname = "forum_containerOf_post";
    info = new one2many_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<1);
    info->flag2 = (1<<4);
    ++p_info;
    
    //Join Date is missing as property
    longname = "forum_hasMember_person";
    shortname = "forum_hasMember_person";
    //info = new dgraph_t;
    info = new p_dgraph_t;
    info->add_edge_property("joinDate", new str_encoder_t);
    //info->add_edge_property("joinDate", new time_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = (1<<1);
    info->flag2 = (1<<2);
    ++p_info;
    
    longname = "forum_hasModerator_person";
    shortname = "forum_hasModerator_person";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<1);
    info->flag2 = (1<<2);
    ++p_info;
    
    longname = "forum_hasTag_tag";
    shortname = "forum_hasTag_tag";
    info = new dgraph_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<1);
    info->flag2 = (1<<5);
    ++p_info;
    
    longname = "organisation_isLocatedIn_place";
    shortname = "organisation_isLocatedIn_place";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<7);
    info->flag2 = (1<<3);
    ++p_info;

    longname = "person_hasInterest_tag";
    shortname = "person_hasInterest_tag";
    info = new dgraph_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    info->flag2 = (1<<5);
    ++p_info;
    
    longname = "person_isLocatedIn_place";
    shortname = "person_isLocatedIn_place";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    info->flag2 = (1<<3);
    ++p_info;
    
    //creation Date
    longname = "person_knows_person";
    shortname = "person_knows_person";
    //info = new ugraph_t;
    info = new p_ugraph_t;
    info->add_edge_property("creationDate", new str_encoder_t);
    //info->add_edge_property("creationDate", new time_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = (1<<2);
    info->flag2 = (1<<2);
    ++p_info;
   
    //creation date 
    longname = "person_likes_comment";
    shortname = "person_likes_comment";
    //info = new dgraph_t;
    info = new p_dgraph_t;
    info->add_edge_property("creationDate", new str_encoder_t);
    //info->add_edge_property("creationDate", new time_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = (1<<2);
    info->flag2 = (1<<0);
    ++p_info;
    
    //creation date
    longname = "person_likes_post";
    shortname = "person_likes_post";
    //info = new dgraph_t;
    info = new p_dgraph_t;
    //info->add_edge_property("creationDate", new time_encoder_t);
    info->add_edge_property("creationDate", new str_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->setup_str(1<<30);
    info->flag1 = (1<<2);
    info->flag2 = (1<<4);
    ++p_info;
    
    //class year
    longname = "person_studyAt_organisation";
    shortname = "person_studyAt_organisation";
    //info = new many2one_t;
    info = new p_many2one_t;
    info->add_edge_property("classYear", new int64_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    info->flag2 = (1<<7);
    ++p_info;
    
    //workfrom year
    longname = "person_workAt_organisation";
    shortname = "person_workAt_organisation";
    //info = new dgraph_t;
    info = new p_dgraph_t;
    info->add_edge_property("workFrom", new int64_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    info->flag2 = (1<<7);
    ++p_info;

    longname = "place_isPartOf_place";
    shortname = "place_isPartOf_place";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<3);
    info->flag2 = (1<<3);
    ++p_info;
    
    longname = "tagclass_isSubclassOf_tagclass";
    shortname = "tagclass_isSubclassOf_tagclass";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<6);
    info->flag2 = (1<<6);
    ++p_info;

    longname = "tag_hasType_tagclass";
    shortname = "tag_hasType_tagclass";
    info = new many2one_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<5);
    info->flag2 = (1<<6);
    ++p_info;
   
    /*-------------------- Properties ---------------------------*/
    //Easy target of enum, multiple languages XXX
    longname = "person_speaks_language";
    shortname = "person_speaks_language";
    //info = new labelkv_t;
    //info->add_edge_property(longname, new embedstr_encoder_t);
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    ++p_info;

    //XXX : multiple email ids
    longname = "person_email_emailaddress";
    shortname = "person_email_emailaddress";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    ++p_info;
   
    longname = "creationDate";
    shortname = "creationDate";
    info = new stringkv_t;
    //info = new labelkv_t;
    //info->add_edge_property(longname, new time_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0) | (1<<1) | (1<<2) | (1<<4);
    ++p_info;
    
    longname = "locationIP";
    shortname = "locationIP";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 =(1<<0) | (1<<2) | (1<<4);
    ++p_info;
    
    longname = "browserUsed";
    shortname = "browserUsed";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0)| (1<<2) | (1<<4);
    ++p_info;
    
    
    longname = "content";
    shortname = "content";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0) | (1<<4);
    ++p_info;
    
    longname = "length"; 
    shortname = "length"; 
    info = new numberkv_t<uint64_t>;
    //info = new labelkv_t;
    //info->add_edge_property("length", new int64_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<0) | (1<<4);
    ++p_info;
    
    longname = "title"; 
    shortname = "title"; 
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<1);
    ++p_info;
    
    longname = "type";
    shortname = "type";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<3) | (1<<7);
    ++p_info;
    
    longname = "name";
    shortname = "name";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<3) | (1<<5) | (1<<6) | (1<<7);
    ++p_info;

    longname = "url";
    shortname = "url";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<3) | (1<<5) | (1<<6)| (1<<7);
    ++p_info;
    
    longname = "firstName";
    shortname = "firstName";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    ++p_info;
    
    longname = "lastName";
    shortname = "lastName";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    ++p_info;
    
    longname = "gender";
    shortname = "gender";
    info = new stringkv_t;
    //info = new labelkv_t;
    //info->add_edge_property("gender", new embedstr_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    ++p_info;
    
    longname = "birthday";
    shortname = "birthday";
    info = new stringkv_t;
    //info = new labelkv_t;
    //info->add_edge_property(longname, new time_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<2);
    ++p_info;

    longname = "imageFile";
    shortname = "imageFile";
    info = new stringkv_t;
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<4);
    ++p_info;
    
    longname = "language";
    shortname = "language";
    info = new stringkv_t;
    //info->add_edge_property(longname, new embedstr_encoder_t);
    g->add_columnfamily(info);
    info->add_column(p_info, longname, shortname);
    info->flag1 = (1<<4);
    ++p_info;
}
/*
static void test1()
{
    const char* src = "person32985348838898";
    const char* pred = "person_knows_person"; 
    
    query_clause query;
    query_whereclause qwhere;
    
    
    query_triple qt;
    qt.set_src(src);
    qt.set_pred(pred);
    qt.set_dst("?x", 0);
    qt.set_traverse(eTransform);
    qt.set_query(&query);

    qwhere.add_child(&qt);
    query.add_whereclause(&qwhere);
    query.setup_qid(1, 1);

    srset_t* srset = query.get_srset(0);
    srset->setup_select(1);
    srset->create_select(0, "?x", 0);
    
    g->run_query(&query);
}

static void test2()
{
    const char* src = "person933"; 
    const char* pred = "person_knows_person"; 
    
    query_clause query;
    query_whereclause qwhere;
    
    
    query_triple qt;
    qt.set_src(src);
    qt.set_pred(pred);
    qt.set_dst("?x", 0);
    qt.set_traverse(eTransform);
    qt.set_query(&query);

    qwhere.add_child(&qt);
    query.add_whereclause(&qwhere);
    query.setup_qid(1, 1);

    srset_t* srset = query.get_srset(0);
    srset->setup_select(1);
    srset->create_select(0, "?x", 0);
    
    g->run_query(&query);

}

static void test3()
{
    const char* dst = "person933"; 
    const char* pred = "comment_hasCreator_person"; 
    
    query_clause query;
    query_whereclause qwhere;
    
    
    query_triple qt;
    qt.set_dst(dst);
    qt.set_pred(pred);
    qt.set_src("?x", 0);
    qt.set_traverse(eTransform);
    qt.set_query(&query);

    qwhere.add_child(&qt);
    query.add_whereclause(&qwhere);
    query.setup_qid(1, 1);

    srset_t* srset = query.get_srset(0);
    srset->setup_select(1);
    srset->create_select(0, "?x", 0);
    
    g->run_query(&query);
}

static void test4()
{
    const char* src = "person933"; 
    const char* pred = "person_speaks_language"; 
    
    query_clause query;
    query.setup_qid(1, 1);
    srset_t* srset = query.get_srset(0);
    srset->setup_select(2);
    srset->create_select(0, "?x", 0);
    srset->create_select(1, "?Y1", pred);
            
    sid_t sid = g->get_sid(src);
    if (sid == INVALID_SID) assert(0);
    tid_t tid = TO_TID(sid);
    srset->setup(tid);
    srset->rset->setup_frontiers(tid, 1);
    srset->add_frontier(sid);

    
    g->run_query(&query);
}
static void test_bfs()
{
    
    const char* pred = "person_knows_person";
	propid_t cf_id = g->get_cfid(pred);
    p_ugraph_t* graph = (p_ugraph_t*)g->cf_info[cf_id];
	tid_t tid = g->get_tid("person");
	sid_t root = TO_SUPER(tid);
	bfs<lite_edge_t>(graph->sgraph, graph->sgraph, root);
    
}

static void test_pagerank()
{
    
    const char* pred = "person_knows_person";
	propid_t cf_id = g->get_cfid(pred);
    p_ugraph_t* graph = (p_ugraph_t*)g->cf_info[cf_id];
	pagerank<lite_edge_t>(graph->sgraph, graph->sgraph, 5);
    
}
*/

void just_get_friends()
{
	string src = "person32985348838898";
	string pred = "person_knows_person";
	string fname = "firstName";

	pid_t pid = g->get_pid(pred.c_str());
	pid_t vid = g->get_pid(fname.c_str());
	cout << "Property Id : " << pid << "  " << vid << endl;

	typekv_t* typekv = g->get_typekv();//dynamic_cast<typekv_t*>(g->cf_info[0]);
	//pgraph_t<dst_id_t>* fr_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(pid);
	stringkv_t* fn_graph = (stringkv_t*)g->get_sgraph(vid);


	sid_t user_id = typekv->get_sid(src.c_str());
	tid_t type_id = TO_TID(user_id);

	vid_t type_node_count = typekv->get_type_vcount(6);

	cout << "The node is of type 6" << " and has " << type_node_count << " vertices "<< endl;
	for(vid_t i = 0; i < type_node_count; ++i){
		sid_t tagclass = TO_SUPER(6) + i;
		cout << "Tag CLass name: " << typekv->get_vertex_name(tagclass) << endl;
	}
	/*
	degree_t fr_count = fr_graph->get_degree_out(user_id);
	
	sid_t* fr_list = (sid_t*) calloc(sizeof(sid_t), fr_count);
	fr_graph->get_nebrs_out(user_id, fr_list);

	for(degree_t i = 0; i < fr_count; ++i){
		sid_t nid = get_sid(fr_list[i]);
		cout << typekv->get_vertex_name(get_sid(fr_list[i])) << endl;
		cout << "First Name :" << fn_graph->get_value(nid) << endl;
	}*/
}

bool sortbysec(const pair<int,int> &a, const pair<int,int> &b){
	return (a.second < b.second);
}

bool osortbysec(const pair<int,int> &a, const pair<int,int> &b){
	return (a.second > b.second);
}

bool sortbystr(const pair<sid_t, string> &a, const pair<sid_t, string> &b){
	return (a.second < b.second);
}

bool osortbystr(const pair<sid_t, string> &a, const pair<sid_t, string> &b){
	return (a.second > b.second);
}
bool sortbydouble(const pair<sid_t, double> &a, const pair<sid_t, double> &b){
	return (a.second < b.second);
}

bool osortbydouble(const pair<sid_t, double> &a, const pair<sid_t, double> &b){
	return (a.second > b.second);
}

struct sort_by_first{
	inline bool operator()(const std::vector<string>vec1, const std::vector<string> vec2){
		return (vec1[0] < vec2[0]);
	}
};

std::vector<sid_t> intersection(std::vector<sid_t> &v1, std::vector<sid_t> &v2){
	std::vector<sid_t> v3;
	std::sort(v1.begin(), v1.end());
	std::sort(v2.begin(), v2.end());

	std::set_intersection(v1.begin(),v1.end(),v2.begin(),v2.end(),back_inserter(v3));
	return v3;
}

/*
 * 13. Single shortest path.
 * Given PersonX and PersonY, find the shortest path between them in the subgraph 
 * induced by the Knows relationships.  Return the length of this path.
 */
void query13()
{
	
	string src = "person32985348838898";
    string dest = "person933";

	// all we need is friendship graph, and type graph
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	
	
	sid_t personX = typekv->get_sid(src.c_str());
	sid_t personY = typekv->get_sid(dest.c_str());

	// start BFS from src, do it untill you reach the dst
	vid_t gnode_count = typekv->get_type_vcount(2);
	// Mark all the vertices as not visited 
	bool *visited = new bool[2*gnode_count];
	int *parent = new int[2*gnode_count];
	// Initialize parent[] and visited[] 
	for (vid_t i = 0; i < 2*gnode_count; i++){
		visited[i] = false;
		parent[i] = -1;
	}

	cout << "The node count on friendship graph is " << gnode_count << endl;
	// Create a queue for BFS 
	std::list<sid_t> queue; 
	       
	// Mark the current node as visited and enqueue it 
	visited[TO_VID(personX)] = true;
	queue.push_back(personX);
	std::list<sid_t>::iterator itr;
	while(!queue.empty()){
		sid_t s = queue.front();
		if(s == personY){
			cout << "The path is : " << endl;
			// print the shortest path
			sid_t curr_node = s;
			while(parent[TO_VID(curr_node)] != -1){
				cout << typekv->get_vertex_name(curr_node) << " <-- ";
				curr_node = parent[TO_VID(curr_node)];
			}
			cout << typekv->get_vertex_name(personX) << endl;

		}
		queue.pop_front();

		// get degree and then neighbors
		degree_t fr_count = fr_graph->get_degree_out(s);
		if(fr_count <= 0) continue; 
		lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
		fr_graph->get_nebrs_out(s, fr_list);

		// loop through neighbors
		for(degree_t i = 0; i < fr_count; ++i){
			if (visited[TO_VID(get_sid(fr_list[i]))] == false){
				visited[TO_VID(get_sid(fr_list[i]))] = true;
				queue.push_back(get_sid(fr_list[i]));
				parent[TO_VID(get_sid(fr_list[i]))] = s;
			}
		}	
	}
}


/*
 * 12. Expert Search. 
 * Find friends of a Person who have replied the most to posts with a tag in a given
 * TagCategory. Return top 20 persons, sorted descending by number of replies.
 */
void query12()
{
	string src = "person32985348838898";
	string tagcategory = "tagclass3";

	// the friendship graph 
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());
    sid_t tag_id = typekv->get_sid(tagcategory.c_str());

    string posttag_str = "post_hasTag_tag"; pid_t posttag_pid = g->get_pid(posttag_str.c_str());
	string postreply_str = "comment_replyOf_post"; pid_t postreply_pid = g->get_pid(postreply_str.c_str());
  	string commenter_str = "comment_hasCreator_person"; pid_t commenter_pid = g->get_pid(commenter_str.c_str());
    string tagcategory_str = "tag_hasType_tagclass"; pid_t tagcategory_pid = g->get_pid(tagcategory_str.c_str());
    
	pgraph_t<dst_id_t>* pt_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(posttag_pid);
	pgraph_t<dst_id_t>* pr_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(postreply_pid);
	pgraph_t<dst_id_t>* cc_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(commenter_pid);
	pgraph_t<dst_id_t>* tc_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(tagcategory_pid);

	
	std::set<sid_t> alltag;
	degree_t tag_count = tc_graph->get_degree_in(tag_id);
	if(tag_count >0){
		dst_id_t* tags = (dst_id_t*) calloc(sizeof(sid_t), tag_count);
		tc_graph->get_nebrs_in(tag_id, tags);
		for(degree_t j = 0; j < tag_count; ++j){
			alltag.insert(get_sid(tags[j]));
		}
	}
	

	// get the friends posts
	std::vector<std::pair<sid_t, double> > person_score;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count > 0){
		lite_edge_t* fr_list = (lite_edge_t*) calloc(fr_count, sizeof(lite_edge_t));
		fr_graph->get_nebrs_out(src_id, fr_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < fr_count; ++i){
			sid_t neighbor = get_sid(fr_list[i]);

			double no_of_replies = 0;
			// For everyone, get all of their comments
			degree_t comm_count = cc_graph->get_degree_in(neighbor);
			if(comm_count <= 0){continue;}
			dst_id_t* comments = (dst_id_t*) calloc(sizeof(dst_id_t), comm_count);
			cc_graph->get_nebrs_in(neighbor, comments);
			for(degree_t j = 0; j < comm_count; ++j){
				sid_t comment = get_sid(comments[j]);
				
				// get post that is reply of a comment; skip the degree
				dst_id_t* posts = (dst_id_t*) calloc(sizeof(dst_id_t), 1);
				if (0 == pr_graph->get_nebrs_out(comment, posts)) continue;
				sid_t post = get_sid(posts[0]);

				// now get the tag of than posts 
				degree_t tag_count = pt_graph->get_degree_out(post);
				if(tag_count <= 0){continue;}
				dst_id_t* tags = (dst_id_t*) calloc(sizeof(dst_id_t), tag_count);
				if (0 == pt_graph->get_nebrs_out(post, tags)) continue;
				for(degree_t k = 0; k < tag_count; ++k){
					sid_t tag = get_sid(tags[k]);
					if(alltag.find(tag) != alltag.end()){
						no_of_replies += 1;
						break;
					}
				}
			}
			person_score.push_back(std::make_pair(neighbor, no_of_replies));
		}
	}

	sort(person_score.begin(), person_score.end(), osortbydouble);
	//print the sorted values
	std::vector<std::pair<sid_t, double> >::iterator itr = person_score.begin();
	int count = 0;
	for(; itr != person_score.end(); ++itr){
		cout << "Person: " << typekv->get_vertex_name(itr->first) << " Replied with: " << itr->second << " comments" << endl;
		if(count > 20){	break; }
		else{ count++; }
	}	
}

/*
 * 11. Job recommendation 
 * Find top 10 friends of the specified Person, or a friend of her friend 
 * (excluding the specified person), who has long worked in a company in a specified
 * Country. Sort ascending by start date, and then ascending by person identifier.
 */
void query11()
{
	string src = "person32985348838898";
	string place = "place87";
	
	// the friendship graph 
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());

	// get the friends posts
	std::set<sid_t> all_friends;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count > 0){
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		all_friends.insert(neighbor);
		degree_t fr_fr_count = fr_graph->get_degree_out(neighbor);
		if (fr_fr_count <= 0){continue;}
		lite_edge_t* fr_fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_fr_count);
		fr_graph->get_nebrs_out(neighbor, fr_fr_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < fr_fr_count; ++i){
			sid_t nneighbor = get_sid(fr_fr_list[i]);
			all_friends.insert(nneighbor);
		}
	}
	// remove myself
	all_friends.erase(src_id);
	}
	// get all organizations from given place
	std::set<sid_t> org_set;
	string orgloc = "organisation_isLocatedIn_place"; pid_t orgloc_pid = g->get_pid(orgloc.c_str());
	pgraph_t<dst_id_t>* orgloc_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(orgloc_pid);
	
	degree_t org_count = orgloc_graph->get_degree_in(typekv->get_sid(place.c_str()));
	if (org_count > 0){
	dst_id_t* org_list = (dst_id_t*) calloc(sizeof(dst_id_t), org_count);
	for(degree_t i = 0; i < org_count; ++i){
		org_set.insert(get_sid(org_list[i]));
		cout << get_sid(org_list[i]) << endl;
	}
	}
	// now for each person check the organization they are working
	string orgnz = "person_workAt_organisation"; pid_t orgnz_pid = g->get_pid(orgnz.c_str());
	pgraph_t<lite_edge_t>* orgnz_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(orgnz_pid);


	std::vector<std::pair<sid_t, double> > worksFrom;
	std::set<sid_t>::iterator f_iter;
	for(f_iter = all_friends.begin(); f_iter != all_friends.end(); ++f_iter){
		sid_t person_id = *f_iter;

		// get the organizations person works for and are in country x
		degree_t orgnz_count = orgnz_graph->get_degree_out(person_id);
		if(orgnz_count <= 0){continue;}
		lite_edge_t* orgnz_list = (lite_edge_t*)calloc(sizeof(lite_edge_t), orgnz_count);
		orgnz_graph->get_nebrs_out(person_id, orgnz_list);

		for(degree_t i = 0; i < orgnz_count; ++i){
			sid_t orgnz_id = get_sid(orgnz_list[i]);

			// the organization is in specific place, then record person and date
			if (org_set.find(orgnz_id) != org_set.end()){
				// get second
				double startdate = orgnz_list[i].second.value_double;
				worksFrom.push_back(std::make_pair(person_id, startdate));
			}
			/*
			// check country of the organization
			degree_t loc_count = orgloc_graph->get_ptr(orgnz_id.second);
			if(loc_count <= 0){continue;}
			sid_t* loc_list = (sid_t*)calloc(sizeof(sid_t), loc_count);
			orgloc_graph->get_nebrs_in(orgnz_id, loc_list);
			for (degree_t j = 0; j < loc_count; ++j){
				if(loc_list[j] == mycountry){

					cout << loc_count << "\t" << typekv->get_vertex_name(orgnz_id) << "\t" << typekv->get_vertex_name(person_id) << endl;
				}
			}
			*/
		}
	}
	sort(worksFrom.begin(), worksFrom.end(), osortbydouble);
	//print the sorted values
	std::vector<std::pair<sid_t, double> >::iterator itr = worksFrom.begin();
	int count = 0;
	for(; itr != worksFrom.end(); ++itr){
		cout << "Person: " << typekv->get_vertex_name(itr->first) << " Worked since: " << itr->second << endl;
		if(count > 20){	break; }
		else{ count++; }
	}	
	
}

/*
 * 10. Friend recommendation.
 *
 * Find top 10 friends of a friend who  posts  much  about  the  interests  of
 * Person and  little  about not interesting topics for the user.  The search is restricted by the
 * candidate’s horoscopeSign.  Returns friends for whom the difference between 
 * the total number of their posts about the interests of the specified user and the 
 * total number of their posts about topics that are not interests of the user,
 * is as large as possible. Sort the result descending by this difference.
 */
void query10(){
	string src = "person32985348838898";
	
	// the friendship graph 
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());

	// get the friends posts
	std::set<sid_t> all_friends;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count == 0){return;}
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		//all_friends.push_back(neighbor);
		degree_t fr_fr_count = fr_graph->get_degree_out(neighbor);
		if (fr_fr_count <= 0){continue;}
		lite_edge_t* fr_fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_fr_count);
		fr_graph->get_nebrs_out(neighbor, fr_fr_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < fr_fr_count; ++i){
			sid_t nneighbor = get_sid(fr_fr_list[i]);
			all_friends.insert(nneighbor);
		}
	}

	// interest tag graph
	string interests = "person_hasInterest_tag"; pid_t interests_pid = g->get_pid(interests.c_str());
	pgraph_t<dst_id_t>* interests_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(interests_pid);
	
	// get my interests
	std::vector<sid_t> my_interests;
	degree_t interest_count = interests_graph->get_degree_out(src_id);
	if (interest_count <= 0){return;}
	dst_id_t* myint = (dst_id_t*) calloc(sizeof(dst_id_t), interest_count);
	interests_graph->get_nebrs_out(src_id, myint);
	for (degree_t i = 0; i < interest_count; ++i){
		my_interests.push_back(get_sid(myint[i]));
	}

	// get my potential friends interests, score them 
	std::vector<std::pair<sid_t, int> > reco_score;
	std::set<sid_t>::iterator f_iter = all_friends.begin();
	for(; f_iter != all_friends.end(); ++f_iter){
		sid_t person_id = *f_iter;

		// now get the tags of each person
		std::vector<sid_t> his_interests;
		degree_t count = interests_graph->get_degree_out(person_id);
		if (count <= 0){continue;}
		dst_id_t* hisint = (dst_id_t*) calloc(sizeof(dst_id_t), count);
		interests_graph->get_nebrs_out(person_id, hisint);
		for (degree_t i = 0; i < count; ++i){
			his_interests.push_back(get_sid(hisint[i]));
		}

		// intersect with my interests; score = common interests - different interests
		std::vector<sid_t> v3 = intersection(my_interests, his_interests);
		int score = v3.size() - (his_interests.size() - v3.size());

		reco_score.push_back(std::make_pair(person_id, score));
	}

	// now sort the score in descending order
	sort(reco_score.begin(), reco_score.end(), osortbysec);
	// print the first 20
	for(int j = 0; j < 20; ++j){
		cout << "Friend recommended: " << typekv->get_vertex_name(reco_score[j].first) << "   Recommendation Score: " << reco_score[j].second << endl;
	}
}

/*
 * 9. Latest Posts.
 * Find the most recent 20 posts and comments from all friends, or friends-of-friends of
 * Person, but created before a Date. Return posts, their creators and creation dates, sort
 * descending by creation date.
 */
void query9(){
	string src = "person32985348838898";
	
	// the friendship graph 
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());

	// get the friends posts
	std::vector<sid_t> all_friends;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count <= 0){return;}
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		all_friends.push_back(neighbor);
		degree_t fr_fr_count = fr_graph->get_degree_out(neighbor);
		if (fr_fr_count <= 0){return;}
		lite_edge_t* fr_fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_fr_count);
		fr_graph->get_nebrs_out(neighbor, fr_fr_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < fr_fr_count; ++i){
			sid_t nneighbor = get_sid(fr_fr_list[i]);
			all_friends.push_back(nneighbor);
		}
	}
	
	string mydate = "2012-09-15T23:29:28.681+0000";
	string post_str = "post_hasCreator_person"; pid_t post_pid = g->get_pid(post_str.c_str());
	pgraph_t<dst_id_t>* post_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(post_pid);
	string comment_str = "comment_hasCreator_person"; pid_t comment_pid = g->get_pid(comment_str.c_str());
	pgraph_t<dst_id_t>* comment_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(comment_pid);
	// creation date value graph
	string ctime_str = "creationDate"; pid_t ctime_pid = g->get_pid(ctime_str.c_str());
	stringkv_t* ctime_graph = (stringkv_t*) g->get_sgraph(ctime_pid);
	

	std::vector<std::pair<sid_t, string> > content_date;
	// for each friends in list, get posts and comments before date
	std::vector<sid_t>::iterator v_iter = all_friends.begin();
	for (; v_iter != all_friends.end(); ++v_iter){
		sid_t person_id = *v_iter;
		//cout << "I am  " << person_id << endl;
		
		// POSTS
		degree_t post_count = post_graph->get_degree_in(person_id);
		//cout << "Number of posts is " << post_count << endl;
		if (post_count <= 0){continue;}
		dst_id_t* post_list = (dst_id_t*) calloc(sizeof(dst_id_t), post_count);
		post_graph->get_nebrs_in(person_id, post_list);
		for (degree_t i = 0; i < post_count; ++i){
			sid_t post_id = get_sid(post_list[i]);
			string date = ctime_graph->get_value(post_id);
			//cout << "creation date is " << date << endl;
			if (date < mydate){
		//		cout << "I Got in!! " << date << endl;
				content_date.push_back(make_pair(post_id, date) );
			}
		}

		// COMMENTS 
		degree_t comment_count = comment_graph->get_degree_in(person_id);
		//cout << "Number of comments is " << comment_count << endl;
		if (comment_count <= 0){continue;}
		dst_id_t* comment_list = (dst_id_t*) calloc(sizeof(dst_id_t), comment_count);
		comment_graph->get_nebrs_in(person_id, comment_list);
		for (degree_t i = 0; i < comment_count; ++i){
			sid_t comment_id = get_sid(comment_list[i]);
			string date = ctime_graph->get_value(comment_id);
			//cout << "creation date is " << date << endl;
			if (date < mydate){
				//cout << "I got in !!!" << date << endl;
				content_date.push_back(make_pair(comment_id, date) );
			}
		}
		//cout << "Size of content date " << content_date.size() << endl;
		
	}

	// now we pushed all the posts and comments and their creation date, sort it descendingly
	sort(content_date.begin(), content_date.end(), osortbystr);
	// print top 20
	std::vector<std::pair<sid_t, string> >::iterator itr = content_date.begin();
	int count = 0;
	for(; itr != content_date.end(); ++itr){
		cout << "Post/comment: " << typekv->get_vertex_name(itr->first) << " Creation date: " << itr->second << endl;
		if(count > 20){	break; }
		else{ count++; }
	}	
}


/*
 * 8.Most recent replies.
 * This  query  retrieves  the  20  most recent reply comments 
 * to all the posts and comments of Person ordered descending by creation date.
 */

void query8()
{
	string src = "person32985348838898";

	//op graphs
	string post_str = "post_hasCreator_person"; pid_t post_pid = g->get_pid(post_str.c_str());
	pgraph_t<dst_id_t>* post_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(post_pid);
	string comment_str = "comment_hasCreator_person"; pid_t comment_pid = g->get_pid(comment_str.c_str());
	pgraph_t<dst_id_t>* comment_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(comment_pid);
	
	// reply graphs 
	string postr_str = "comment_replyOf_post"; pid_t postr_pid = g->get_pid(postr_str.c_str());
	pgraph_t<dst_id_t>* postr_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(postr_pid);
	string commentr_str = "comment_replyOf_comment"; pid_t commentr_pid = g->get_pid(commentr_str.c_str());
	pgraph_t<dst_id_t>* commentr_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(commentr_pid);
	
	// creation date value graph
	string ctime_str = "creationDate"; pid_t ctime_pid = g->get_pid(ctime_str.c_str());
	stringkv_t* ctime_graph = (stringkv_t*) g->get_sgraph(ctime_pid);
	
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());

	// get posts created by a person
	std::vector<std::pair<sid_t,string > > all_replies;
	degree_t post_count = post_graph->get_degree_in(src_id);
	//cout << "Number of posts is " << post_count << endl;
	if (post_count > 0){
	dst_id_t* post_list = (dst_id_t*) calloc(sizeof(dst_id_t), post_count);
	post_graph->get_nebrs_in(src_id, post_list);
	for (degree_t i = 0; i < post_count; ++i){
		sid_t post_id = get_sid(post_list[i]);
		// for each post find it's replies, put it in a vector
		degree_t preply_count = postr_graph->get_degree_in(post_id);
		if (preply_count <= 0){return;}
		dst_id_t* preply_list = (dst_id_t*) calloc(sizeof(dst_id_t), preply_count);
		postr_graph->get_nebrs_in(post_id, preply_list);

		// now register individual replies to vector
		for (degree_t j = 0; j < preply_count; ++j){
		//	cout << "Reply of post" << typekv->get_vertex_name(preply_list[j]);
			string time = ctime_graph->get_value(get_sid(preply_list[j]));
			all_replies.push_back(std::make_pair(get_sid(preply_list[j]), time));
		}
	}
	}
	// get comments created by a person
	degree_t comment_count = comment_graph->get_degree_in(src_id);
	//cout << "Number of comments is " << comment_count << endl;
	if (comment_count > 0){
	dst_id_t* comment_list = (dst_id_t*) calloc(sizeof(dst_id_t), comment_count);
	comment_graph->get_nebrs_in(src_id, comment_list);
	for(degree_t i = 0; i < comment_count; ++i){
		sid_t comment_id = get_sid(comment_list[i]);
		// for each comment find it's replies, put it in vector
		degree_t creply_count = commentr_graph->get_degree_in(comment_id);
		if (creply_count <= 0){continue;}
		dst_id_t* creply_list = (dst_id_t*) calloc(sizeof(dst_id_t), creply_count);
		commentr_graph->get_nebrs_in(comment_id, creply_list);

		// now register individual replies to vector
		for (degree_t j = 0; j < creply_count; ++j){
		//	cout << "Reply of comment" << typekv->get_vertex_name(creply_list[j]);
			string time = ctime_graph->get_value(get_sid(creply_list[j]));
			all_replies.push_back(std::make_pair(get_sid(creply_list[j]), time));
		}
	}
	}

	sort(all_replies.begin(), all_replies.end(), osortbystr);
	// now we have all posts and comments sort them by timestamp
	std::vector< std::pair<sid_t, string> >::iterator v_iter = all_replies.begin();
	for(; v_iter != all_replies.end(); ++v_iter){
		cout <<"Reply: " << typekv->get_vertex_name(v_iter->first)  << " Replied at: "<< v_iter->second << endl;
	}
}

/*
 * 7. Recent likes.
 * For the specified Person get the most recent likes  of  any  of  the  person’s  posts,
 * and  the  latency  between  the corresponding  post  and  the  like.   Flag  Likes  
 * from  outside  the direct connections. 
 * Return top 20 Likes, ordered descending by creation date of the like
 */

void query7()
{
	string src = "person933";
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);

	//op graphs
	string post_str = "post_hasCreator_person"; pid_t post_pid = g->get_pid(post_str.c_str());
	pgraph_t<dst_id_t>* post_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(post_pid);
	string comment_str = "comment_hasCreator_person"; pid_t comment_pid = g->get_pid(comment_str.c_str());
	pgraph_t<dst_id_t>* comment_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(comment_pid);
	
    string clike_str = "person_likes_comment"; pid_t clike_pid = g->get_pid(clike_str.c_str());
    string plike_str = "person_likes_post"; pid_t  plike_pid = g->get_pid(plike_str.c_str());
	pgraph_t<lite_edge_t>* plike_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(plike_pid);
	pgraph_t<lite_edge_t>* clike_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(clike_pid);

	// List a person's direct connections
	std::set<sid_t> all_friends;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count <= 0){return;}
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		all_friends.insert(neighbor);
	}

	// get all posts and comments from the person
	std::vector<std::pair<sid_t, sid_t> > indirect_liker; // post_id, person_id
	std::vector<std::pair<sid_t, double> > timed_liker; // liker_person_id, timestamp

	// post first
	degree_t p_count = post_graph->get_degree_in(src_id);
	if (p_count > 0){
	dst_id_t* p_list = (dst_id_t*) calloc(sizeof(dst_id_t), p_count);
	post_graph->get_nebrs_in(src_id, p_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < p_count; ++i){
		sid_t post_id = get_sid(p_list[i]);

		// get all likers of this post, flag if not from friends
		degree_t num_likes = plike_graph->get_degree_in(post_id);
		if (num_likes <= 0){continue;}
		lite_edge_t* like_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), num_likes);
		plike_graph->get_nebrs_in(post_id, like_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < num_likes; ++i){
			sid_t person_id = get_sid(like_list[i]);
			// check if he is my friend
			if(all_friends.find(person_id) == all_friends.end()){
				indirect_liker.push_back(std::make_pair(post_id, person_id));
			}
			// get time of like 
			double time_like = like_list[i].second.value_double;
			timed_liker.push_back(std::make_pair(person_id, time_like));

		}
		//all_contents.push_back(neighbor);
	}}
	// comment second
	degree_t c_count = comment_graph->get_degree_in(src_id);
	if (c_count > 0){
	dst_id_t* c_list = (dst_id_t*) calloc(sizeof(dst_id_t), c_count);
	comment_graph->get_nebrs_in(src_id, c_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < c_count; ++i){
		sid_t comment_id = get_sid(c_list[i]);
		
		// get all likers of this post, flag if not from friends
		degree_t num_likes = clike_graph->get_degree_in(comment_id);
		if (num_likes <= 0){continue;}
		lite_edge_t* like_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), num_likes);
		clike_graph->get_nebrs_in(comment_id, like_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < num_likes; ++i){
			sid_t person_id = get_sid(like_list[i]);
			// check if he is my friend
			if(all_friends.find(person_id) == all_friends.end()){
				indirect_liker.push_back(std::make_pair(comment_id, person_id));
			}
			// get time of like  
			double time_like = like_list[i].second.value_double;
			timed_liker.push_back(std::make_pair(person_id, time_like));

		}
		//all_contents.push_back(neighbor);
	}}

	// 2. flag the likes outside of direct connection
	cout << "Indirect Likers " << endl;
	std::vector<std::pair<sid_t, sid_t> >::iterator itr = indirect_liker.begin(); // post_id, person_id
	for(; itr != indirect_liker.end(); ++itr){
		try{
		cout << "Post: " << typekv->get_vertex_name(itr->first) << " Liked by: " << typekv->get_vertex_name(itr->second) << endl;
		}catch(...){
		cout << "";
		}
	}

	cout << "Most recent Likers " << endl;
	std::sort(timed_liker.begin(), timed_liker.end(), osortbydouble);
	std::vector<std::pair<sid_t, double> >::iterator itr1 = timed_liker.begin(); // post_id, person_id
	for(; itr1 != timed_liker.end(); ++itr1){
		try{
		cout << "Liked by: " << typekv->get_vertex_name(itr1->first) << " Liked on: " << itr1->second << endl;
		}catch(...){
			cout <<"";
		}
	}

}
/*
 * 6. Tag co-occurrence. 
 * Given a start Person and some Tag,find the other Tags that occur together with this Tag on Posts
 * that were created by Person’s friends and friends of friends. Return top 10 Tags, 
 * sorted descending by the count of Posts that were created by these Persons, which 
 * contain both this Tag and the given Tag.
 */
void query6()
{
	string src = "person32985348838898";
	string tag = "tag1732";
	
	// the friendship graph 
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());
	sid_t tag_id = typekv->get_sid(tag.c_str());

	// get the friends posts
	std::vector<sid_t> all_friends;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count <= 0){return;}
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		all_friends.push_back(neighbor);
		degree_t fr_fr_count = fr_graph->get_degree_out(neighbor);
		if (fr_fr_count <= 0){continue;}
		lite_edge_t* fr_fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_fr_count);
		fr_graph->get_nebrs_out(neighbor, fr_fr_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < fr_fr_count; ++i){
			sid_t nneighbor = get_sid(fr_fr_list[i]);
			all_friends.push_back(nneighbor);
		}
	}
	//cout << "It's here" << endl;
	// now list all the posts from these peoples that has tag listed above that contains the tag
	
	string post_str = "post_hasCreator_person"; pid_t post_pid = g->get_pid(post_str.c_str());
	pgraph_t<dst_id_t>* post_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(post_pid);
	string tag_str = "post_hasTag_tag"; pid_t tag_pid = g->get_pid(tag_str.c_str());
	pgraph_t<dst_id_t>* tag_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(tag_pid);
	
	std::map<sid_t, uint64_t> tagCooccuranceFreq;
	for(std::vector<sid_t>::iterator f_iter = all_friends.begin(); f_iter != all_friends.end(); ++f_iter){
		sid_t person_id = *f_iter;
		degree_t post_count = post_graph->get_degree_in(person_id);
		if (post_count <= 0){continue;}
		dst_id_t* post_list = (dst_id_t*) calloc(sizeof(dst_id_t), post_count);
		post_graph->get_nebrs_in(person_id, post_list);
		//cout << "Person: " << typekv->get_vertex_name(person_id) << " Degree: " << post_count << endl;;

		for (degree_t k = 0; k < post_count; ++k){
			sid_t post_id = get_sid(post_list[k]);
			// look for all the tags in the post, check if original tag is in it
			degree_t tag_count = tag_graph->get_degree_out(post_id);
			if (tag_count <= 0){continue;}
			dst_id_t* tag_list = (dst_id_t*) calloc(sizeof(dst_id_t), tag_count);
			tag_graph->get_nebrs_out(post_id, tag_list);

			degree_t j = 0;
			for (; j < tag_count; ++j){
				//cout << typekv->get_vertex_name(tag_list[j]) << endl;
				if(get_sid(tag_list[j]) == tag_id) {break;}
			}
			if (j < tag_count){
				// this means the tag is in the post so go over and update all values
				for (degree_t i = 0; i < tag_count; ++i){
					std::map<sid_t, uint64_t>::iterator m_iter = tagCooccuranceFreq.find(get_sid(tag_list[i]));
					if (m_iter == tagCooccuranceFreq.end()){
						tagCooccuranceFreq[get_sid(tag_list[i])] = 1;
					}
					else{
						m_iter->second++;	
					}
				}
			}
		}
	}
	
	//cout << "Iy's here as well!!" << endl;
	// copy to vec, sort by co-occurance freq
	std::vector<std::pair< sid_t, uint64_t> > tagFreqPair;
	for(std::map<sid_t, uint64_t>::iterator m_iter = tagCooccuranceFreq.begin(); m_iter != tagCooccuranceFreq.end(); ++m_iter){
		tagFreqPair.push_back(std::make_pair(m_iter->first, m_iter->second));
	}
	std::sort(tagFreqPair.begin(), tagFreqPair.end(), osortbysec);
	for(degree_t i = 0; i < 20; ++i){
		cout << "Tag: " << typekv->get_vertex_name(tagFreqPair[i].first) << "Co-occurance Frequency: " << tagFreqPair[i].second << endl;
	}
}

/*
 * 5. New groups. 
 * Given a start Person, find the top 20 Forums the 
 * friends and friends of friends of that Person joined after a
 * given Date. Sort results descending by the number of Posts in
 * each Forum that were created by any of these Persons.
 */
void query5()
{
	// get friends and friends of friends- one list
	string src = "person32985348838898";
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());

	// get the friends posts
	std::vector<sid_t> all_friends;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count <= 0){return;}
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		all_friends.push_back(neighbor);
		degree_t fr_fr_count = fr_graph->get_degree_out(neighbor);
		if (fr_fr_count <= 0){continue;}
		lite_edge_t* fr_fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_fr_count);
		fr_graph->get_nebrs_out(neighbor, fr_fr_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < fr_fr_count; ++i){
			sid_t nneighbor = get_sid(fr_fr_list[i]);
			all_friends.push_back(nneighbor);
		}
	}
	// for all forums, check forum has member list - one per forum, intersect with your friends
	degree_t forumcount = typekv->get_type_vcount(1);
	std::map<sid_t, std::vector<sid_t> > friendsinforum;
	string forum_str = "forum_hasMember_person"; pid_t forum_pid = g->get_pid(forum_str.c_str());
	pgraph_t<lite_edge_t>* forum_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(forum_pid);

	for (degree_t i = 0; i < forumcount; ++i){
		sid_t forum_id = TO_SUPER(1) + i;
		degree_t f_member_count = forum_graph->get_degree_out(forum_id);
		if (f_member_count <= 0) continue;
		lite_edge_t* f_member_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), f_member_count);
		forum_graph->get_nebrs_out(forum_id, f_member_list);

		std::vector<sid_t> my_members;
		for(degree_t j = 0; j < f_member_count; ++j){
			my_members.push_back(get_sid(f_member_list[j]));
		}
		std::vector<sid_t> member_friends = intersection(my_members, all_friends);
		if (!member_friends.empty()){
			friendsinforum[forum_id] = member_friends;
		}
	}
/*	std::map<sid_t, std::vector<sid_t> >::iterator x_iter = friendsinforum.begin();
	for(; x_iter != friendsinforum.end(); ++x_iter){
		cout << "Tag: " << typekv->get_vertex_name(x_iter->first) << " Frequency: " << x_iter->second.size() << endl;
	}
*/
	cout << "Members sorted out " << endl;
	// for all forums, list all posts made by your friends
	string fpost_str = "forum_containerOf_post"; pid_t fpost_pid = g->get_pid(fpost_str.c_str());
	pgraph_t<dst_id_t>* fpost_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(fpost_pid);

	string ppost_str = "post_hasCreator_person"; pid_t ppost_pid = g->get_pid(ppost_str.c_str());
	pgraph_t<dst_id_t>* ppost_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(ppost_pid);
	
	std::vector<std::pair<sid_t, uint64_t> > forumscore;
	std::map<sid_t, std::vector<sid_t> >::iterator fm_iter = friendsinforum.begin();
	for(; fm_iter != friendsinforum.end(); ++fm_iter){
		sid_t forum_id = fm_iter->first;
		std::vector<sid_t> f_members = fm_iter->second;

		// first find all posts in forum
		std::vector<sid_t> post_vec1;
		degree_t f_post_count = fpost_graph->get_degree_out(forum_id);
		if(f_post_count <= 0){continue;}
		dst_id_t* f_post_list = (dst_id_t*) calloc(sizeof(dst_id_t), f_post_count);
		fpost_graph->get_nebrs_out(forum_id, f_post_list);
		for(int i = 0; i < f_post_count; ++i){
			post_vec1.push_back(get_sid(f_post_list[i]));
		}

		// now list all the posts from friends in that forum
		std::vector<sid_t> post_vec2;
		for(std::vector<sid_t>::iterator v_iter = f_members.begin(); v_iter != f_members.end(); ++v_iter){
			sid_t person_id = *v_iter;
			degree_t p_post_count = ppost_graph->get_degree_in(person_id);
			if(p_post_count <= 0){continue;}
			dst_id_t* p_post_list = (dst_id_t*) calloc(sizeof(dst_id_t), p_post_count);
			ppost_graph->get_nebrs_in(person_id, p_post_list);
			for(int i = 0; i < p_post_count; ++i){
				post_vec2.push_back(get_sid(f_post_list[i]));
			}
		}

		// now intersect two lists, thats our score
		std::vector<sid_t> post_vec3 = intersection(post_vec1, post_vec2);
		forumscore.push_back(std::make_pair(forum_id, post_vec3.size()));
	}
	cout << "Posts collected " << endl;
	// sort forumid-score descending
	std::sort(forumscore.begin(), forumscore.end(), osortbysec);
	for(degree_t i = 0; i < 20; ++i){
		cout << "Tag: " << typekv->get_vertex_name(forumscore[i].first) << " Frequency: " << forumscore[i].second << endl;
	}
	
}


/*
 * 4. New Topics. 
 * Given a start Person, find the top 10 mostpopular Tags
 * (by total number of posts with the tag) that are attached 
 * to Posts that were created by that Person’s friends within
 * a given time interval.
 */
void query4()
{
	string src = "person32985348838898";
	string friends = "person_knows_person"; pid_t fr_pid = g->get_pid(friends.c_str());
	string posts = "post_hasCreator_person"; pid_t post_pid = g->get_pid(posts.c_str());
	string tag = "post_hasTag_tag"; pid_t tag_pid = g->get_pid(tag.c_str());
	
	pgraph_t<dst_id_t>* post_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(post_pid);
	pgraph_t<dst_id_t>* tag_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(tag_pid);
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(fr_pid);
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());

	// get the friends posts
	std::vector<sid_t> all_posts;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count <= 0){return;}
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		// get all of his posts
		degree_t p_count = post_graph->get_degree_in(neighbor);
		if (p_count > 0){
			dst_id_t* p_list = (dst_id_t*) calloc(sizeof(dst_id_t), p_count);
			post_graph->get_nebrs_in(neighbor, p_list);
			for(degree_t j = 0; j < p_count; ++j){
				sid_t post_id = get_sid(p_list[j]);
				all_posts.push_back(post_id);
			}
		}
	}
	// now go over the posts and find tag frequencies
	std::map<sid_t, uint64_t>tagFrequency;
	std::map<sid_t, uint64_t>::iterator m_iter; 
	for(std::vector<sid_t>::iterator p_iter = all_posts.begin(); p_iter != all_posts.end(); ++p_iter){
		sid_t post_id = *p_iter;
		degree_t tag_count = tag_graph->get_degree_out(post_id);
		if(tag_count == 0) continue;
		dst_id_t* tag_list = (dst_id_t*) calloc(sizeof(dst_id_t), tag_count);
		tag_graph->get_nebrs_out(post_id, tag_list);
		for(degree_t i = 0; i < tag_count; ++i){
			sid_t tag_id = get_sid(tag_list[i]);
			m_iter = tagFrequency.find(tag_id);
			if (m_iter != tagFrequency.end()){
				m_iter->second++;
			}
			else{
				tagFrequency[tag_id] = 1;
			}
		}
	}
	// sort tags by frequencies
	// copy map to pair vector
	std::vector<std::pair< sid_t, uint64_t> > tagFreqPair;
	for(std::map<sid_t, uint64_t>::iterator m_iter = tagFrequency.begin(); m_iter != tagFrequency.end(); ++m_iter){
		tagFreqPair.push_back(std::make_pair(m_iter->first, m_iter->second));
	}
	std::sort(tagFreqPair.begin(), tagFreqPair.end(), osortbysec);
	for(degree_t i = 0; i < 20; ++i){
		cout << "Tag: " << typekv->get_vertex_name(tagFreqPair[i].first) << " Frequency: " << tagFreqPair[i].second << endl;
	}
}

/* 3. Friends within 2 steps that recently traveled to countries X and Y. 
 * Find top 20 friends and friends of friends of a given Person
 * who have made a post or a comment in the foreign Country X
 * and Country Y within a specified period of Duration In Days after
 * a startDate. Sorted results descending by total number of posts and comments.
 */
void query3()
{
	// get required graph names 
	string src = "person32985348838898";
	string pred = "person_knows_person";
	string mydate = "2012-09-15T23:29:28.681+0000";


	string posts = "post_hasCreator_person"; pid_t post_pid = g->get_pid(posts.c_str());
	string comments = "comment_hasCreator_person"; pid_t comment_pid = g->get_pid(comments.c_str());
	string date = "creationDate"; pid_t date_pid = g->get_pid(date.c_str());
    string p_place = "post_isLocatedIn_place"; pid_t pp_pid = g->get_pid(p_place.c_str());
    string c_place = "comment_isLocatedIn_place"; pid_t cp_pid = g->get_pid(c_place.c_str());
	
	pgraph_t<dst_id_t>* post_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(post_pid);
	pgraph_t<dst_id_t>* comment_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(comment_pid);
	pgraph_t<dst_id_t>* pplace_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(pp_pid);
	pgraph_t<dst_id_t>* cplace_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(cp_pid);
	stringkv_t* date_graph = (stringkv_t*)g->get_sgraph(date_pid);
	
	// get friends and friend of friends 
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());
	pid_t pid = g->get_pid(pred.c_str());
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(pid);
		
	string cX = "place50", cY = "place25";
	sid_t countryX = typekv->get_sid(cX.c_str());
	sid_t countryY = typekv->get_sid(cY.c_str());
	std::set<sid_t> fromX;
	std::set<sid_t> fromY;
	
	degree_t p_count = pplace_graph->get_degree_in(countryX);
	if (p_count > 0){
		dst_id_t* p_list = (dst_id_t*) calloc(sizeof(dst_id_t), p_count);
		pplace_graph->get_nebrs_in(countryX, p_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < p_count; ++i){
			fromX.insert(get_sid(p_list[i]));
		}
	}
	degree_t c_count = cplace_graph->get_degree_in(countryX);
	if (c_count > 0){
		dst_id_t* c_list = (dst_id_t*) calloc(sizeof(dst_id_t), c_count);
		cplace_graph->get_nebrs_in(countryX, c_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < c_count; ++i){
			fromX.insert(get_sid(c_list[i]));
		}
	}
	p_count = pplace_graph->get_degree_in(countryY);
	if (p_count > 0){
		dst_id_t* p_list = (dst_id_t*) calloc(sizeof(dst_id_t), p_count);
		pplace_graph->get_nebrs_in(countryY, p_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < p_count; ++i){
			fromY.insert(get_sid(p_list[i]));
		}
	}
	c_count = cplace_graph->get_degree_in(countryY);
	if (c_count > 0){
		dst_id_t* c_list = (dst_id_t*) calloc(sizeof(dst_id_t), c_count);
		cplace_graph->get_nebrs_in(countryY, c_list);
		// loop through friends, get posts and comments 
		for(degree_t i = 0; i < c_count; ++i){
			fromY.insert(get_sid(c_list[i]));
		}
	}

	std::vector<sid_t> dr;
	// get friends
	std::vector<sid_t> friends;
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	if (fr_count <= 0){return;}
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);
	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		friends.push_back(neighbor);
		
		// get friends of friend
		degree_t fr_fr_count = fr_graph->get_degree_out(neighbor);
		if (fr_fr_count <= 0){continue;}
		lite_edge_t* fr_fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_fr_count);
		fr_graph->get_nebrs_out(neighbor, fr_fr_list);
		for (degree_t j = 0; j < fr_fr_count; ++j){
			sid_t n_neighbor = get_sid(fr_fr_list[j]);
			friends.push_back(n_neighbor);
		}
	}
	// sort with no of post/comment
	// TODO apply the date filter ; date not working currently
	std::vector<std::pair<sid_t, degree_t> > pnc_count;
	for(std::vector<sid_t>::iterator f_iter = friends.begin(); f_iter != friends.end(); ++f_iter){
		degree_t p_count = post_graph->get_degree_in(*f_iter);
		degree_t c_count = comment_graph->get_degree_in(*f_iter);

		degree_t total = p_count + c_count;
		pnc_count.push_back(std::make_pair(*f_iter, total));
	}
	sort(pnc_count.begin(), pnc_count.end(), sortbysec);

	// find the ones with multiple countries untill you get 20
	std::vector<std::pair<sid_t, degree_t> >::iterator f_iter = pnc_count.begin();
	for(; f_iter != pnc_count.end(); ++f_iter){
		sid_t neighbor = f_iter->first;
		sid_t content_count = f_iter->second;
		if (content_count <= 0){continue;}

		std::set<int> countryset;
		countryset.clear();

		// now check if there is at least one post/comment from X and one from Y
		bool isfromX = false;
		bool isfromY = false;

		// Posts
		degree_t p_count = post_graph->get_degree_in(neighbor);
		if (p_count > 0){
			dst_id_t* p_list = (dst_id_t*) calloc(sizeof(dst_id_t), p_count);
			post_graph->get_nebrs_in(neighbor, p_list);
			for(degree_t j = 0; j < p_count; ++j){
				sid_t post = get_sid(p_list[j]);
				if (fromX.find(post) != fromX.end()){isfromX = true;}
				if (fromY.find(post) != fromY.end()){isfromY = true;}
			}
		}

		//comments
		degree_t c_count = comment_graph->get_degree_in(neighbor);
		if (c_count>0){
			dst_id_t* c_list = (dst_id_t*) calloc(sizeof(dst_id_t), c_count);
			comment_graph->get_nebrs_in(neighbor, c_list);
			for(degree_t j = 0; j < c_count; ++j){
				sid_t comment = get_sid(c_list[j]);
				if (fromX.find(comment) != fromX.end()){isfromX = true;}
				if (fromY.find(comment) != fromY.end()){isfromY = true;}
			}
		}

		if((isfromX == true) && (isfromY == true)){
			// add details to data result vector, break if size is > 20 
			dr.push_back(neighbor);
			cout << typekv->get_vertex_name(neighbor) << endl;
			if (dr.size() > 20){
				return;
			}
		}
	}
}

/*
 * 2. Find the newest 20 posts and comments from your friends.
 * Given a start Person, find (most recent) Posts and Comments
 * from all of that Person’s friends, that were created before (and
 * including) a given Date. Return the top 20 Posts/Comments,and
 * the Person that created each of them.  Sort results descending by
 * creation date, and then ascending by Post identifier.
 */
void query2()
{
	// get required graph names 
	string src = "person32985348838898";
	string pred = "person_knows_person";
	string mydate = "2012-09-15T23:29:28.681+0000";

	string posts = "post_hasCreator_person"; pid_t post_pid = g->get_pid(posts.c_str());
	string comments = "comment_hasCreator_person"; pid_t comment_pid = g->get_pid(comments.c_str());
	string date = "creationDate"; pid_t date_pid = g->get_pid(date.c_str());
	pgraph_t<dst_id_t>* post_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(post_pid);
	pgraph_t<dst_id_t>* comment_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(comment_pid);
	stringkv_t* date_graph = (stringkv_t*)g->get_sgraph(date_pid);
	
	// get friends 
	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	sid_t src_id = typekv->get_sid(src.c_str());
	pid_t pid = g->get_pid(pred.c_str());
	//pgraph_t<dst_id_t>* fr_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(pid);
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(pid);
		
	// get friends
	degree_t fr_count = fr_graph->get_degree_out(src_id);
	//if(fr_count <= 0){return;}
	//sid_t* fr_list = (sid_t*) calloc(sizeof(sid_t), fr_count);
	lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
	fr_graph->get_nebrs_out(src_id, fr_list);

	std::vector< std::vector<string> > dr;

	// loop through friends, get posts and comments 
	for(degree_t i = 0; i < fr_count; ++i){
		sid_t neighbor = get_sid(fr_list[i]);
		
		// Posts
		degree_t p_count = post_graph->get_degree_in(neighbor);
		std::vector<string> temp;
		if (p_count > 0){
			dst_id_t* p_list = (dst_id_t*) calloc(sizeof(dst_id_t), p_count);
			post_graph->get_nebrs_in(neighbor, p_list);
			for(degree_t j = 0; j < p_count; ++j){
				temp.clear();
				sid_t post = get_sid(p_list[j]);
				string creationdate = date_graph->get_value(post);
				if(creationdate > mydate){continue;}
				//cout << typekv->get_vertex_name(post) <<"\t"<< creationdate << endl;
				temp.push_back(creationdate);
				temp.push_back(typekv->get_vertex_name(post));
				temp.push_back(typekv->get_vertex_name(neighbor));
				dr.push_back(temp);
			}
		}

		//comments
		degree_t c_count = comment_graph->get_degree_in(neighbor);
		if (c_count>0){
			dst_id_t* c_list = (dst_id_t*) calloc(sizeof(dst_id_t), c_count);
			comment_graph->get_nebrs_in(neighbor, c_list);
			for(degree_t j = 0; j < c_count; ++j){
				temp.clear();
				sid_t comment = get_sid(c_list[j]);
				string creationdate = date_graph->get_value(comment);
				if (creationdate > mydate){continue;}
				//cout << typekv->get_vertex_name(comment) <<"\t"<< creationdate << endl;
				temp.push_back(creationdate);
				temp.push_back(typekv->get_vertex_name(comment));
				temp.push_back(typekv->get_vertex_name(neighbor));
				dr.push_back(temp);
			}
		}
	}
	std::sort(dr.begin(), dr.end(), sort_by_first());
	for(int i = 0; i < 20; ++i){
		cout << dr[i][0] << "\t" << dr[i][1] << "\t" << dr[i][2] << endl;
	}
}

/*
 * 1. Extract description of friends with a given name:
 *
 * Given a person’s firstName, return up to 20 people with the same first
 * name, sorted by increasing distance (max 3) from a given person, and 
 * for  people  within  the  same  distance  sorted  by  last  name. Give details 
 * of each person
 */
void query1()
{
	// get source node, firstname = read firstname
	string src = "person32985348838898";
	string pred = "person_knows_person";

	// load graphs corresponding to individual properties
	const char* val[7];
	val[0] = "firstName";
	val[1] = "lastName";
	val[2] = "birthday";
	val[3] = "creationDate";
	val[4] = "gender";
	val[5] = "browserUsed";
	val[6] = "locationIP";

	stringkv_t* val_graph[9]; 
	for (int i = 0; i < 9; ++i){
		pid_t vid = g->get_pid(val[i]);
		val_graph[i] = (stringkv_t*)g->get_sgraph(vid);
	}

	typekv_t* typekv = dynamic_cast<typekv_t*>(g->cf_info[0]);
	pid_t pid = g->get_pid(pred.c_str());
	//pgraph_t<dst_id_t>* fr_graph = (pgraph_t<dst_id_t>*)g->get_sgraph(pid);
	pgraph_t<lite_edge_t>* fr_graph = (pgraph_t<lite_edge_t>*)g->get_sgraph(pid);


	// get starting point, and pivot full name
	sid_t src_id = typekv->get_sid(src.c_str());
	const char* firstname = val_graph[0]->get_value(src_id);
	cout << "Required name is " << firstname << endl;

	uint8_t count = 0, dist = 0;
	std::vector<std::vector<sid_t> > res_id;
	std::vector<sid_t> prev_frontier;
	prev_frontier.push_back(src_id);
	std::vector<sid_t> next_frontier;

	// Status array
	vid_t gnode_count = typekv->get_type_vcount(TO_TID(src_id));
	uint8_t* visit_status = (uint8_t*) calloc(sizeof(uint8_t), gnode_count);
	for(vid_t i = 0; i < gnode_count; ++i){
		visit_status[i] = 0;
	}

	++dist;
	while (!prev_frontier.empty()){
		std::vector<uint64_t> tresult;
		std::vector<uint64_t>::iterator f_iter = prev_frontier.begin();
		for(; f_iter != prev_frontier.end(); ++f_iter){
			vid_t x = TO_VID(*f_iter);
			visit_status[x] = 1;

			// get degree
			degree_t fr_count = fr_graph->get_degree_out(*f_iter);
			if(fr_count <= 0) continue; 
			lite_edge_t* fr_list = (lite_edge_t*) calloc(sizeof(lite_edge_t), fr_count);
			fr_graph->get_nebrs_out(*f_iter, fr_list);
			// loop through neighbors
			for(degree_t i = 0; i < fr_count; ++i){
				sid_t neighbor = get_sid(fr_list[i]);
				if (visit_status[TO_VID(neighbor)] == 1){
					continue;
				}else{
					const char* fname = val_graph[0]->get_value(neighbor);
					//cout << "Obtained name is " << fname << endl;
					if (0 == strncmp(firstname, fname, strlen(firstname))){
						++count;
						tresult.push_back(neighbor);
					}
				}
				if (count >= 20) break;
			}
			if (count >= 20) break;
		}
		prev_frontier = next_frontier;
		next_frontier.clear();
		res_id.push_back(tresult);
		++dist;
		if ((count >= 20) ||(dist > 3)) break;
	}
	cout << "Exploration Completed" << endl;

	// Detail grabbing 
	std::vector<std::vector<string> > dr;
	for(size_t d_iter = 0; d_iter < res_id.size(); ++d_iter){
		// for given distance, build a intermediate container
		std::vector<std::vector<string> > dr_bin;
		std::vector<uint64_t> id_bin = res_id[d_iter];
		for(std::vector<uint64_t>::iterator id_iter = id_bin.begin(); id_iter != id_bin.end(); ++id_iter){

			cout << typekv->get_vertex_name(*id_iter) << endl;
			
			std::vector<string> myres;
			// Get all the craps, insert it into myres
			for(int j = 1; j < 7; j++){
				string value;
				try{
					value = val_graph[j]->get_value(*id_iter);
				}catch(...){
					value = "NA";
				}
				myres.push_back(value);
				cout << value << " ";
			}
			cout << val_graph[0]->get_value(*id_iter) << endl;
			myres.push_back(val_graph[0]->get_value(*id_iter));
			dr_bin.push_back(myres);
			
		}
		// sort using a different function
		std::sort(dr_bin.begin(), dr_bin.end(), sort_by_first());
		dr.insert(dr.end(), dr_bin.begin(), dr_bin.end());
	}

	// print first
	cout <<"Description " << "\t" << "First " << "\t" << "Last " << endl;
	for(size_t i = 0; i < 7; ++i){
		cout << "\t"<< dr[0][i] << "\t" << dr[dr.size()-1][i] << endl; 
	}	
	// return dr;
}

void ldbc_test0(const string& conf_file, const string& idir, const string& odir)
{
    schema_ldbc();
    csv_manager::prep_graph(conf_file, idir, odir);
    cout << "----------Test 4-----------------" << endl;
	
	//just_get_friends();
	//cout << "----------XXX --------------------" << endl;
	//assert(0);
	query1();
	cout << "----------Query 1----------------" << endl;
	query2();
	cout << "----------Query 2----------------" << endl;
	query3();
	cout << "----------Query 3----------------" << endl;
	query4();
	cout << "----------Query 4----------------" << endl;
	query5();
	cout << "----------Query 5----------------" << endl;
	query6();// not working yet
	cout << "----------Query 6----------------" << endl;
	query7();
	cout << "----------Query 7----------------" << endl;
	query8();
	cout << "----------Query 8----------------" << endl;
	query9();
	cout << "----------Query 9----------------" << endl;
	query10();
	cout << "----------Query 10----------------" << endl;
	query11();
	cout << "----------Query 11----------------" << endl;
	query12();
	cout << "----------Query 12----------------" << endl;
	query13();
	cout << "----------Query 13----------------" << endl;
    /*
    test4();
    cout << "----------Test 1-----------------" << endl;
    test1();
    cout << "----------Test 2-----------------" << endl;
    test2();
    cout << "----------Test 3-----------------" << endl;
    test3();
	cout << "-----------BFS -----------------" << endl;
	test_bfs();
	cout << "-----------Pagerank-------------" << endl;
	test_pagerank();
    */
}

void ldbc_test2(const string& odir)
{
    schema_ldbc();
    g->read_graph_baseline();
    cout << "----------Test 4-----------------" << endl;
    /*
    test4();
    cout << "----------Test 1-----------------" << endl;
    test1();
    cout << "----------Test 2-----------------" << endl;
    test2();
    cout << "----------Test 3-----------------" << endl;
    test3();
	cout << "-----------BFS -----------------" << endl;
	test_bfs();
	cout << "-----------Pagerank-------------" << endl;
	test_pagerank();
    */
}

void ldbc_test(const string& conf_file, const string& idir, const string& odir, int job) 
{
    switch(job) {
        case 0: 
            ldbc_test0(conf_file, idir, odir);
            break;
        case 1:
            ldbc_test2(odir);
            break;
        default:
            cout << "No testcase selected" << endl;
            break;
    }
}

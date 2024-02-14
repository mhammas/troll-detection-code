import json

def recursive_lookup(k, d):
    if k in d:
        return d[k]
    for v in d.values():
        if isinstance(v, dict):
            return recursive_lookup(k, v)
    return None

arranged_threads = {}
id_to_post_dict = {}

print('Loading Comments')
for i in range(0, 8):
	f = open('/data/hammas/filtered_comments_linkid/shortlisted_user_comments_' + str(i) + '.json', 'r')
	print('File Number: ', str(i))
	for line in f:
		json_line = json.loads(line)
		post_id = json_line['id']
		if post_id in id_to_post_dict:
			print('Duplicate Comment Found')
		else:
			id_to_post_dict[post_id] = json_line

print('Total Comments: ', len(id_to_post_dict))
id_to_post_dict_copy = id_to_post_dict.copy()

print('Arranging Comments in Threads')

print('ITERATION 1')
for p_id in id_to_post_dict:
	post_object = id_to_post_dict[p_id]
	parent_id = post_object['parent_id']
	link_id = post_object['link_id']

	if parent_id == link_id:
		#HEAD-LEVEL COMMENT
		arranged_threads[p_id] = {}
		del id_to_post_dict_copy[p_id]

print(len(id_to_post_dict_copy))

elements_to_store = len(id_to_post_dict_copy)
id_to_post_dict_copy_copy = id_to_post_dict_copy.copy()
thread_dict = arranged_threads

iteration_count = 2
previous_elements_store = len(id_to_post_dict_copy)
while elements_to_store != 0:
	print('ITERATION: ', str(iteration_count))
	temp_thread_dict = {}

	for p_id in id_to_post_dict_copy:
		post_object = id_to_post_dict_copy[p_id]
		parent_id_without_t3 = post_object['parent_id'][3:]

		if parent_id_without_t3 in thread_dict:
			new_memory = {}
			thread_dict[parent_id_without_t3][p_id] = new_memory
			del id_to_post_dict_copy_copy[p_id]
			elements_to_store -= 1
			temp_thread_dict[p_id] = new_memory

	print(len(id_to_post_dict_copy_copy))
	id_to_post_dict_copy = id_to_post_dict_copy_copy.copy()
	thread_dict = temp_thread_dict
	iteration_count += 1

	if elements_to_store != previous_elements_store:
		previous_elements_store = elements_to_store
	else:
		break

	#if elements_to_store == 100:
		#print(id_to_post_dict_copy_copy[0])
		#break

for p_id in id_to_post_dict_copy_copy:
	#print(p_id, recursive_lookup(p_id, arranged_threads))
	link_id = id_to_post_dict_copy_copy[p_id]['link_id']
	#print(link_id == 't3_3dr99b')

with open('arranged_threads1.json', 'w') as f:
    json.dump(arranged_threads, f)


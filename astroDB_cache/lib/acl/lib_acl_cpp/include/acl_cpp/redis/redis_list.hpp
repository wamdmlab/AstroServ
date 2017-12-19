#pragma once
#include "acl_cpp/acl_cpp_define.hpp"
#include "acl_cpp/redis/redis_command.hpp"

namespace acl
{

class redis_client;

class ACL_CPP_API redis_list : virtual public redis_command
{
public:
	/**
	 * see redis_command::redis_command()
	 */
	redis_list(void);

	/**
	 * see redis_command::redis_command(redis_client*)
	 */
	redis_list(redis_client* conn);

	/**
	 * see redis_command::redis_command(redis_client_cluster*, size_t)
	 */
	redis_list(redis_client_cluster* cluster, size_t max_conns = 0);

	virtual ~redis_list(void);

	/////////////////////////////////////////////////////////////////////

	/**
	 * �� key �б�����е���һ��Ԫ�ض���name/value�ԣ�������������ʽ��ͷ��������
	 * ��������� key ����ʱ�������� key ���Ⱥ�˳�����μ������б�������һ��
	 * �ǿ��б��ͷԪ��
	 * remove and get a element from list head, or block until one
	 * is available; when multiple keys were given, multiple elements
	 * will be gotten according the sequence of keys given.
	 * @param result {std::pair<string, string>&} �洢���Ԫ�ض��󣬸ö����
	 *  ��һ���ַ�����ʾ���б����� key���ڶ���Ϊ�ö����ͷ��Ԫ��
	 *  store the elements result, the first string of pair is the key,
	 *  and the second string of pair is the element
	 * @param timeout {size_t} �ȴ�����ʱ�䣨�룩���ڳ�ʱʱ����û�л��Ԫ�ض���
	 *  �򷵻� false�������ֵΪ 0 ��һֱ�ȴ������Ԫ�ض�������
	 *  the blocking timeout in seconds before one element availble;
	 *  false will be returned when the timeout is arrived; if the timeout
	 *  was set to be 0, this function will block until a element was
	 *  available or some error happened.
	 * @param first_key {const char*} ��һ���ǿ��ַ����� key �������һ������
	 *  ������ NULL ��������ʾ����б�Ľ���
	 *  the first key of a variable args, the last arg must be NULL
	 *  indicating the end of the variable args.
	 * @return {bool} �Ƿ�����ͷ��Ԫ�ض���������� false �������¿���ԭ��
	 *  true if got a element in the head of list, when false was be
	 *  returned, there'are some reasons show below:
	 *  1������
	 *     error happened.
	 *  2����һ�� key ���б����
	 *     at least one key was not a list object.
	 *  3��key �����ڻ�ʱδ���Ԫ�ض���
	 *     key not exist or timeout was got.

	 */
	bool blpop(std::pair<string, string>& result, size_t timeout,
		const char* first_key, ...);
	bool blpop(const std::vector<const char*>& keys, size_t timeout,
		std::pair<string, string>& result);
	bool blpop(const std::vector<string>& keys, size_t timeout,
		std::pair<string, string>& result);

	/**
	 * ����μ� blpop��Ψһ����Ϊ�÷�������β��Ԫ�ض���
	 * the meaning is same as the blpop above except that this function
	 * is used to pop element from the tail of the list
	 * @see blpop
	 */
	bool brpop(std::pair<string, string>& result, size_t timeout,
		const char* first_key, ...);
	bool brpop(const std::vector<const char*>& keys, size_t timeout,
		std::pair<string, string>& result);
	bool brpop(const std::vector<string>& keys, size_t timeout,
		std::pair<string, string>& result);

	/**
	 * ����ʽִ����������������
	 * 1) ���б� src �е����һ��Ԫ��(βԪ��)�����������ظ��ͻ��ˡ�
	 * 2) �� src ������Ԫ�ز��뵽�б� dst ����Ϊ dst �б�ĵ�ͷԪ��
	 * two actions will be executed in blocking mode as below:
	 * 1) pop a element from src list's tail, and return it to caller
	 * 2) push the element to dst list's head
	 * @param src {const char*} Դ�б���� key
	 *  the key of source list
	 * @param dst {const char*} Ŀ���б���� key
	 *  the key of destination list
	 * @param buf {string*} �ǿ�ʱ�洢 src ��β��Ԫ�� key ֵ
	 *  if not NULL, buf will store the element poped from the head of src
	 * @param timeout {size_t} �ȴ���ʱʱ�䣬���Ϊ 0 ��һֱ�ȴ�ֱ�������ݻ����
	 *  the timeout to wait, if the timeout is 0 this function will
	 *  block until a element was available or error happened.
	 * @return {bool} ���� src �б��гɹ�����β��Ԫ�ز����� dst �б�ͷ����
	 *  �÷������� true������ false ��ʾ��ʱ������� src/dst ��һ�����б����
	 *  true if success, false if timeout arrived, or error happened,
	 *  or one of the src and dst is not a list object
	 * @see rpoplpush
	 */
	bool brpoplpush(const char* src, const char* dst, size_t timeout,
		string* buf = NULL);

	/**
	 * ���� key ��Ӧ���б�����У�ָ���±��Ԫ��
	 * return the element of the specified subscript from the list at key
	 * @param key {const char*} �б����� key
	 *  the key of one list object
	 * @param idx {size_t} �±�ֵ
	 *  the specified subscript
	 * @param buf {string&} �洢���
	 *  store the result
	 * @return {bool} ���� true ���������ɹ�����ʱ�� buf ���ݷǿ��������ȷ�����
	 *  ָ���±��Ԫ�أ���� buf.empty()��ʾû�л��Ԫ�أ����� false ʱ��������ʧ��
	 *  true if success, and if buf is empty, no element was got;
	 *  false if error happened
	 */
	bool lindex(const char* key, size_t idx, string& buf);

	/**
	 * ���б�����н�һ����Ԫ�������ָ��Ԫ�ص�ǰ��
	 * insert one new element before the specified element in list
	 * @param key {const char*} �б����� key
	 *  the key of specified list
	 * @param pivot {const char*} �б�����е�һ��ָ��Ԫ����
	 *  the speicifed element of list
	 * @param value {const char*} �µ�Ԫ����
	 *  the new element to be inserted
	 * @reutrn {int} ���ظ��б�����Ԫ�ظ������������£�
	 *  return the number of list specified by the given key, as below:
	 *  -1 -- ��ʾ����� key ���б����
	 *        error happened or the object of the key is not a list
	 *  >0 -- ��ǰ�б�����Ԫ�ظ���
	 *        the number of elements of the specified list
	 */
	int linsert_before(const char* key, const char* pivot,
		const char* value);
	int linsert_before(const char* key, const char* pivot,
		size_t pivot_len, const char* value, size_t value_len);

	/**
	 * ���б�����н�һ����Ԫ�������ָ��Ԫ�صĺ���
	 * append a new element after the specified element in the list
	 * @param key {const char*} �б����� key
	 *  the key of the specified list
	 * @param pivot {const char*} �б�����е�һ��ָ��Ԫ����
	 *  the specified element
	 * @param value {const char*} �µ�Ԫ����
	 *  the new element
	 * @reutrn {int} ���ظ��б�����Ԫ�ظ������������£�
	 *  return the number of elements in the list specifed by the key:
	 *  -1 -- ��ʾ����� key ���б����
	 *        error happened or it is not a list object specified by key
	 *  >0 -- ��ǰ�б�����Ԫ�ظ���
	 *        the number of elements in list specified by the key
	 */
	int linsert_after(const char* key, const char* pivot,
		const char* value);
	int linsert_after(const char* key, const char* pivot,
		size_t pivot_len, const char* value, size_t value_len);

	/**
	 * ����ָ���б�����Ԫ�ظ���
	 * get the number of elements in list specified the given key
	 * @param key {const char*} �б����� key
	 *  the list's key
	 * @return {int} ����ָ���б����ĳ��ȣ���Ԫ�ظ������� -1 if error happened
	 *  return the number of elements in list, -1 if error
	 */
	int llen(const char* key);

	/**
	 * ���б�������Ƴ�������ͷ��Ԫ��
	 * remove and get the element in the list's head
	 * @param key {const char*} Ԫ�ض���� key
	 *  the key of one list
	 * @param buf {string&} �洢������Ԫ��ֵ
	 *  store the element when successful.
	 * @return {int} ����ֵ���壺1 -- ��ʾ�ɹ�����һ��Ԫ�أ�-1 -- ��ʾ�������
	 *  ������б���󣬻�ö����Ѿ�Ϊ��
	 *  return value as below:
	 *   1: get one element successfully
	 *  -1: error happened, or the oject is not a list specified
	 *      by the key, or the list specified by key is empty
	 */
	int lpop(const char* key, string& buf);

	/**
	 * ��һ������ֵԪ�ز��뵽�б���� key �ı�ͷ
	 * add one or more element(s) to the head of a list
	 * @param key {const char*} �б����� key
	 *  the list key
	 * @param first_value {const char*} ��һ���ǿ��ַ������ñ�ε��б�����һ��
	 *  ������Ϊ NULL
	 *  the first no-NULL element of the variable args, the last arg must
	 *  be NULL indicating the end of the args.
	 * @return {int} ����������ǰ�б�����е�Ԫ�ظ��������� -1 ��ʾ������ key
	 *  ������б���󣬵��� key ������ʱ������µ��б���󼰶����е�Ԫ��
	 *  return the number of elements in list. -1 if error happened,
	 *  or the object specified by key is not a list.
	 */
	int lpush(const char* key, const char* first_value, ...);
	int lpush(const char* key, const char* values[], size_t argc);
	int lpush(const char* key, const std::vector<string>& values);
	int lpush(const char* key, const std::vector<const char*>& values);
	int lpush(const char* key, const char* values[], const size_t lens[],
		size_t argc);

	/**
	 * ��һ���µ��б�����Ԫ��������Ѿ����ڵ�ָ���б�����ͷ���������б����
	 * ������ʱ�����
	 * add a new element before the head of a list, only if the list exists
	 * @param key {const char*} �б����� key
	 *  the list's key
	 * @param value {const char*} �¼ӵ��б�����Ԫ��
	 *  the new element to be added
	 * @return {int} ���ص�ǰ�б�����Ԫ�ظ������������£�
	 *  return the number of elements in the list:
	 *  -1��������� key ���б����
	 *      error or the key isn't refer to a list
	 *   0���� key ���󲻴���
	 *      the list specified by the given key doesn't exist
	 *  >0��������ǰ�б�����е�Ԫ�ظ���
	 *      the number of elements in list specifed by key after added
	 */
	int lpushx(const char* key, const char* value);
	int lpushx(const char* key, const char* value, size_t len);

	/**
	 * �����б� key ��ָ�������ڣ������䣩��Ԫ�أ�������ƫ���� start �� end ָ����
	 * �±���ʼֵ�� 0 ��ʼ��-1 ��ʾ���һ���±�ֵ
	 * get a range of elements from list, the range is specified by
	 * start and end, and the start begins with 0, -1 means the end
	 * @param key {const char*} �б����� key
	 *  the specified key of one list
	 * @param start {int} ��ʼ�±�ֵ
	 *  the start subscript of list
	 * @param end {int} �����±�ֵ
	 *  the end subscript of list
	 * @param result {std::vector<string>*} �ǿ�ʱ�洢�б������ָ�������Ԫ�ؼ���
	 *  if not NULL, result will be used to store the results
	 * @return {bool} �����Ƿ�ɹ��������� false ��ʾ����� key ���б����
	 *  if success for this operation, false if the key is not a list or
	 *  error happened
	 *  ������
	 *  for example:
	 *  1) �� start = 0, end = 10 ʱ��ָ�����±� 0 ��ʼ�� 10 �� 11 ��Ԫ��
	 *     if start is 0 and end is 10, then the subscript range is
	 *     between 0 and 10(include 10).
	 *  2) �� start = -1, end = -2 ʱ��ָ�������һ��Ԫ�صڵ����ڶ����� 2 ��Ԫ�� 
	 *     if start is -1 and end is -2, the range is from the end and
	 *     backward the second element.
	 *
	 *  �����ɹ������ͨ��������һ��ʽ�������
	 *  the result can be got by one of the ways as below:
	 *
	 *  1���ڵ��÷����д���ǿյĴ洢�������ĵ�ַ
	 *     the most easily way is to set a non-NULL result parameter
	 *     for this function
	 *  2�����෽�� get_value ���ָ���±��Ԫ������
	 *     get the specified subscript's element by redis_command::get_value 
	 *  3�����෽�� get_child ���ָ���±��Ԫ�ض���(redis_result����Ȼ����ͨ��
	 *     redis_result::argv_to_string �������Ԫ������
	 *     get redis_result object with the given subscript, and get the
	 *     element by redis_result::argv_to_string
	 *  4�����෽�� get_result ����ȡ���ܽ�������� redis_result��Ȼ����ͨ��
	 *     redis_result::get_child ���һ��Ԫ�ض���Ȼ����ͨ����ʽ 2 ��ָ��
	 *     �ķ�����ø�Ԫ�ص�����
	 *     get redis_result object by redis_command::get_result, and get
	 *     the first element by redis_result::get_child, then get the
	 *     element by the way same as the way 2 above.
	 *  5�����෽�� get_children ��ý��Ԫ�����������ͨ�� redis_result ��
	 *     �ķ��� argv_to_string ��ÿһ��Ԫ�ض����л��Ԫ������
	 *     get child array by redis_command::get_children, and get the
	 *     element from one of redis_result array by argv_to_string.
	 */
	bool lrange(const char* key, int start, int end,
		std::vector<string>* result);

	/**
	 * ����Ԫ��ֵ���б�������Ƴ�ָ��������Ԫ��
	 * remove the first count occurrences of elements equal to value
	 * from the list stored at key
	 * @param key {const char*} �б����� key
	 *  the key of a list
	 * @param count {int} �Ƴ�Ԫ�ص��������ƣ�count �ĺ������£�
	 *  the first count of elements to be removed, as below:
	 *  count > 0 : �ӱ�ͷ��ʼ���β�������Ƴ��� value ��ȵ�Ԫ�أ�����Ϊ count
	 *              remove elements equal to value moving from head to tail
	 *  count < 0 : �ӱ�β��ʼ���ͷ�������Ƴ��� value ��ȵ�Ԫ�أ�����Ϊ count �ľ���ֵ
	 *              remove elements equal to value moving from tail to head
	 *  count = 0 : �Ƴ����������� value ��ȵ�ֵ
	 *              remove all elements equal to value
	 * @param value {const char*} ָ����Ԫ��ֵ����Ҫ���б�����б����������ֵ�Ƚ�
	 *  the specified value for removing elements
	 * @return {int} ���Ƴ��Ķ�������������ֵ�������£�
	 *  the count of elements removed, meaning show below:
	 *  -1�������� key ������б����
	 *      error happened or the key is not refer to a list
	 *   0��key �����ڻ��Ƴ���Ԫ�ظ���Ϊ 0
	 *      the key does not exist or the count of elements removed is 0
	 *  >0�����ɹ��Ƴ���Ԫ������
	 *      the count of elements removed successfully
	 */
	int lrem(const char* key, int count, const char* value);
	int lrem(const char* key, int count, const char* value, size_t len);

	/**
	 * ���б� key �±�Ϊ idx ��Ԫ�ص�ֵ����Ϊ value���� idx ����������Χ�����
	 * һ�����б�( key ������)���� lset ʱ������һ������
	 * set the value of a element in a list by its index, if the index
	 * out of bounds or the key of list not exist, an error will happen.
	 * @param key {const char*} �б����� key
	 *  the key of list
	 * @param idx {int} �±�λ�ã���Ϊ��ֵʱ���β����ͷβ����λ���������˳��ʽ��
	 *  �磺0 ��ʾͷ����һ��Ԫ�أ�-1 ��ʾβ����ʼ�ĵ�һ��Ԫ��
	 *  the index in the list, if it's negative, iterating data will be
	 *  from tail to head, or be from head to tail.
	 * @param value {const char*} Ԫ����ֵ
	 *  the new value of the element by its index
	 * @return {bool} �� key ���б����� key �����ڻ� idx ������Χ�򷵻� false
	 *  if success. false if the object of the key isn't list, or key's
	 *  list not exist, or the index out of bounds.
	 */
	bool lset(const char* key, int idx, const char* value);
	bool lset(const char* key, int idx, const char* value, size_t len);

	/**
	 * ��ָ�����б���󣬶�һ���б�����޼������б�ֻ����ָ�������ڵ�Ԫ�أ�
	 * ����ָ������֮�ڵ�Ԫ�ض�����ɾ����������ƫ���� start �� end ָ����
	 * �±���ʼֵ�� 0 ��ʼ��-1 ��ʾ���һ���±�ֵ
	 * remove elements in a list by range betwwen start and end.
	 * @param key {const char*} �б����� key
	 *  the key of a list
	 * @param start {int} ��ʼ�±�ֵ
	 *  the start index in a list
	 * @param end {int} �����±�ֵ
	 *  the end index in a list
	 * @return {bool} �����Ƿ�ɹ��������� false ʱ��ʾ�����ָ���� key �����
	 *  �б���󣻵��ɹ�ɾ���� key ���󲻴���ʱ�򷵻� true
	 *  if success. false if error happened, or the key's object is not
	 *  a list, or the key's object not exist.
	 */
	bool ltrim(const char* key, int start, int end);

	/**
	 * ���б�������Ƴ�������β��Ԫ��
	 * remove and get the last element of a list
	 * @param key {const char*} Ԫ�ض���� key
	 *  the key of the list
	 * @param buf {string&} �洢������Ԫ��ֵ
	 *  store the element pop from list
	 * @return {int} ����ֵ���壺1 -- ��ʾ�ɹ�����һ��Ԫ�أ�-1 -- ��ʾ�������
	 *  ������б���󣬻�ö����Ѿ�Ϊ��
	 *  return as below:
	 *   1: get a element successfully
	 *  -1: error happened, or not a list, or the list is empty.
	 */
	int rpop(const char* key, string& buf);

	/**
	 * ��һ��ԭ��ʱ���ڣ���������ʽִ����������������
	 * ���б� src �е����һ��Ԫ��(βԪ��)�����������ظ��ͻ��ˡ�
	 * �� src ������Ԫ�ز��뵽�б� dst ����Ϊ dst �б�ĵ�ͷԪ��
	 * remove the last element in a list, prepend it to another list
	 * and return it.
	 * @param src {const char*} Դ�б���� key
	 *  the key of the source list
	 * @param dst {const char*} Ŀ���б���� key
	 *  the key of the destination list
	 * @param buf {string*} �ǿ�ʱ�洢 src ��β��Ԫ�� key ֵ
	 *  if not NULL, it will store the element
	 * @return {bool} ���� src �б��гɹ�����β��Ԫ�ز����� dst �б�ͷ����
	 *  �÷������� true������ false ����� src/dst ��һ�����б����
	 *  true if the element was removed from a list to another list,
	 *  false if error happened, one of src or dst is not a list.
	 */
	bool rpoplpush(const char* src, const char* dst, string* buf = NULL);

	/**
	 * ��һ������ֵԪ�ز��뵽�б���� key �ı�β
	 * append one or multiple values to a list
	 * @param key {const char*} �б����� key
	 *  the key of a list
	 * @param first_value {const char*} ��һ���ǿ��ַ������ñ�ε��б�����һ��
	 *  ������Ϊ NULL
	 *  the first element of a variable args must be not NULL, and the
	 *  last arg must be NULL indicating the end of the args.
	 * @return {int} ����������ǰ�б�����е�Ԫ�ظ��������� -1 ��ʾ������ key
	 *  ������б���󣬵��� key ������ʱ������µ��б���󼰶����е�Ԫ��
	 *  return the number of a list specified by a key. -1 if error
	 *  happened, or the key's object isn't a list, if the list by the
	 *  key doese not exist, a new list will be created with the key.
	 */
	int rpush(const char* key, const char* first_value, ...);
	int rpush(const char* key, const char* values[], size_t argc);
	int rpush(const char* key, const std::vector<string>& values);
	int rpush(const char* key, const std::vector<const char*>& values);
	int rpush(const char* key, const char* values[], const size_t lens[],
		size_t argc);

	/**
	 * ��һ���µ��б�����Ԫ��������Ѿ����ڵ�ָ���б�����β���������б����
	 * ������ʱ�����
	 * append one or multiple values to a list only if the list exists.
	 * @param key {const char*} �б����� key
	 *  the key of a list
	 * @param value {const char*} �¼ӵ��б�����Ԫ��
	 *  the new element to be added.
	 * @return {int} ���ص�ǰ�б�����Ԫ�ظ������������£�
	 *  return the number of the list, as below:
	 *  -1��������� key ���б����
	 *      error happened, or the key's object isn't a list
	 *   0���� key ���󲻴���
	 *      the key's object doesn't exist
	 *  >0��������ǰ�б�����е�Ԫ�ظ���
	 *     the number of elements in the list after adding.
	 */
	int rpushx(const char* key, const char* value);
	int rpushx(const char* key, const char* value, size_t len);

private:
	int linsert(const char* key, const char* pos, const char* pivot,
		size_t pivot_len, const char* value, size_t value_len);
	int pushx(const char* cmd, const char* key,
		const char* value, size_t len);
	int pop(const char* cmd, const char* key, string& buf);
	bool bpop(const char* cmd, const std::vector<const char*>& keys,
		size_t timeout, std::pair<string, string>& result);
	bool bpop(const char* cmd, const std::vector<string>& keys,
		size_t timeout, std::pair<string, string>& result);
	bool bpop(std::pair<string, string>& result);
};

} // namespace acl

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <valkey/cluster.h>

static void printReply(valkeyReply *reply);
static valkeyReply * scanCluster(valkeyClusterContext *cc);
static valkeyReply * copyReply(valkeyReply *src);
static valkeyReply * appendReply(valkeyReply *first, valkeyReply *second);

int main(int argc, char **argv) {
	UNUSED(argc);
	UNUSED(argv);

	valkeyClusterContext *cc = valkeyClusterConnect("valkey-1:6379,valkey-2:6379,valkey-3:6379", VALKEYCLUSTER_FLAG_NULL);
	if (cc == NULL || cc->err) {
		fprintf(stderr, "Error: %s\n", cc ? cc->errstr : "OOM");
		return 1;
	}

	valkeyClusterNodeIterator iter;
	valkeyClusterInitNodeIterator(&iter, cc);
	valkeyClusterNode *node = valkeyClusterNodeNext(&iter);
	int dbsize = 0;
	while (node != NULL) {
		valkeyReply *reply = valkeyClusterCommandToNode(cc, node, "DBSIZE");
		if (reply == NULL || reply->type == VALKEY_REPLY_ERROR) {
			fprintf(stderr, "Error: DBSIZE %s\n", cc->err ? cc->errstr : "OOM");
			return 1;
		} else {
			dbsize += reply->integer;
		}
		freeReplyObject(reply);
		
		reply = valkeyClusterCommandToNode(cc, node, "CLUSTER SLOTS");
		if (reply == NULL || reply->type == VALKEY_REPLY_ERROR) {
			fprintf(stderr, "Error: DBSIZE %s\n", cc->err ? cc->errstr : "OOM");
			return 1;
		}
		printf("=== CLUSTER SLOTS =========\n");
		printReply(reply);
		printf("===========================\n");
		freeReplyObject(reply);
		node = valkeyClusterNodeNext(&iter);
	}

	printf("Total DBSIZE: %d\n", dbsize);
	valkeyReply *scanReply = scanCluster(cc);
	if (scanReply == NULL) {
		fprintf(stderr, "Error: SCAN Cluster %s\n", cc->err ? cc->errstr : "OOM");
		return 1;
	}
	printf("printing Scan Reply\n");
	printReply(scanReply);

	valkeyClusterFree(cc);
	return 0;
}

static valkeyReply * scanCluster(valkeyClusterContext *cc) {
	valkeyClusterNodeIterator iter;
	valkeyClusterInitNodeIterator(&iter, cc);
	valkeyClusterNode *node = valkeyClusterNodeNext(&iter);
	valkeyReply *clusterReply = NULL;
	valkeyReply *reply;
	while (node != NULL) {
		reply = valkeyClusterCommandToNode(cc, node, "SCAN 0 COUNT 1000");
		if (reply == NULL || reply->type == VALKEY_REPLY_ERROR) {
			fprintf(stderr, "Error: SCAN %s\n", cc->err ? cc->errstr : "OOM");
			return NULL;
		}
		printf("fetching SCAN from %s/%s\n", node->addr, node->name);
		clusterReply = appendReply(clusterReply, reply);
		node = valkeyClusterNodeNext(&iter);
		freeReplyObject(reply);
	}
	return clusterReply;
}

static valkeyReply * copyReply(valkeyReply *src) {
	valkeyReply *dst = NULL;
	if (src == NULL) {
		printf("src is NULL\n");
		return NULL;
	}
	if (dst == NULL) {
		dst = malloc(sizeof(valkeyReply));
		memset(dst, 0, sizeof(valkeyReply));
		assert(dst != NULL);
	}
	dst->type = src->type;
	dst->elements = src->elements;
	switch (dst->type) {
	case VALKEY_REPLY_STRING:
		dst->str = malloc(src->len);
		memset(dst->str, 0, src->len);
		assert(dst->str != NULL);
		dst->len = src->len;
		memcpy(dst->str, src->str, src->len);
		break;
	case VALKEY_REPLY_INTEGER:
		dst->integer = src->integer;
		break;
	case VALKEY_REPLY_ARRAY:
		dst->element = malloc(sizeof(valkeyReply *) * dst->elements);
		memset(dst->element, 0, sizeof(valkeyReply *) * dst->elements);
		assert(dst->element != NULL);
		// printf("src->elements: %ld\n", dst->elements);
		for (int i = 0; i < dst->elements; i++) {
			dst->element[i] = NULL;
			dst->element[i] = copyReply(src->element[i]);
		}
		break;
	default:
		break;
	}
	return dst;
}

static valkeyReply *
appendReply(valkeyReply *first, valkeyReply *second)
{
	if (second == NULL) {
		printf("second is NULL\n");
		return NULL;
	}
	if (first == NULL) {
		return copyReply(second);
	}
	if (first->type != VALKEY_REPLY_ARRAY || second->type != VALKEY_REPLY_ARRAY) {
		printf("first or second is not an array\n");
		return NULL;
	}
	valkeyReply *dst = malloc(sizeof(valkeyReply));
	memset(dst, 0, sizeof(valkeyReply));
	assert(dst != NULL);
	dst->type = VALKEY_REPLY_ARRAY;
	dst->elements = 2;
	dst->element = malloc(sizeof(valkeyReply *) * dst->elements);
	memset(dst->element, 0, sizeof(valkeyReply *) * dst->elements);
	assert(dst->element != NULL);
	dst->element[0] = NULL;
	dst->element[0] = copyReply(first->element[0]);

	dst->element[1] = NULL;
	dst->element[1] = copyReply(first->element[1]);
	dst->element[1]->elements += second->element[1]->elements;
	dst->element[1]->element = malloc(sizeof(valkeyReply *) * dst->element[1]->elements);
	memset(dst->element[1]->element, 0, sizeof(valkeyReply *) * dst->element[1]->elements);
	assert(dst->element[1]->element != NULL);
	for (int i = 0; i < first->element[1]->elements; i++) {
		dst->element[1]->element[i] = NULL;
		dst->element[1]->element[i] = copyReply(first->element[1]->element[i]);
	}
	for (int i = 0; i < second->element[1]->elements; i++) {
		dst->element[1]->element[first->element[1]->elements + i] = NULL;
		dst->element[1]->element[first->element[1]->elements + i] = copyReply(second->element[1]->element[i]);
	}
	freeReplyObject(first);
	return dst;
}

static void
printReply(valkeyReply *reply) {
	if (reply == NULL) {
		printf("reply is NULL\n");
		return;
	}
	switch (reply->type)
	{
	case VALKEY_REPLY_STRING:
		printf("String: %s\n", reply->str);
		break;
	case VALKEY_REPLY_INTEGER:
		printf("Int: %lld\n", reply->integer);
		break;
	case VALKEY_REPLY_ARRAY:
		printf("Array: %ld\n", reply->elements);
		for (int i = 0; i < reply->elements; i++) {
			printReply(reply->element[i]);
		}
		break;
	default:
		break;
	}
}

// vim: ts=4 sw=4 noet

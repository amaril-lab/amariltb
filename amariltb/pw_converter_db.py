import boto3
from .pw_converter_const import Tables,TablesTest,TablesV2
from boto3.dynamodb.conditions import Key, Attr
from .pw_converter_const import Languages
from .pw_converter_utils import normelize_word
import os
import botocore.session
from decimal import Decimal

# Get the service resource.
# Get the service resource.
# TBD: deal with this (from config file ?)
#dynamodb = boto3.resource('dynamodb',aws_access_key_id='AKIAI5JNMYPCN64VB2AQ',
#    aws_secret_access_key='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', region_name='eu-west-1')
#dynamodb = boto3.resource('dynamodb')


class DynamoDB:
    instance = None
    version = None
  
    def __init__(self):
        pass

    def get_table(self,table_name):
     
        if(DynamoDB.version == "TEST"):
            return TablesTest[table_name]
        if(DynamoDB.version == "V1"):
            return Tables[table_name]
        if(DynamoDB.version == "V2"):
            return TablesV2[table_name]
        raise AssertionError('incorrect table version specified')


    def update_items(self,table,items):
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        return

    def update_Assignment(self,items):
        table = DynamoDB.instance.Table(self.get_table("assignments")) # pylint: disable=no-member
        self.update_items(table,items)
        return

    def update_index(self,items):
        table = DynamoDB.instance.Table(self.get_table("index")) # pylint: disable=no-member
        self.update_items(table,items)
        return

    def to_tranactions(self,items):
        transactions = []
        for item in items:
            action = item.pop("transactionAction",None)
            if(not action):
                raise AssertionError('Error: to_transaction no action on item:'+str(item))
            
            if( not (action in ["Delete","Put","Update","ConditionCheck"] ) ):
                raise AssertionError('Error: to_transaction action is invalid'+str(action))

            trans_item = {}
            for key in item:
                if (key == "index"):
                    trans_item[key] = {
                        "N": str(item["index"]),
                    }
                else:
                    trans_item[key] = {
                        "S": item[key]
                    }

                   
            transaction = {} 

            if(action == 'Delete'):
                transaction[action] = {
                        'TableName': self.get_table("index"),
                        'Key':{'id':trans_item['id']}
                }
                
            else:
                transaction[action] = {
                        'TableName': self.get_table("index"),
                        'Item':trans_item
                }
            
            transactions.append(transaction)
        return transactions
    

    def atomic_update_index(self,items):
        session = botocore.session.get_session()
        client = session.create_client('dynamodb', region_name='eu-west-1')
        transactions = self.to_tranactions(items)
        client.transact_write_items(TransactItems=transactions)
        return

    def get_index_item_by_word(self,index_word,language,category):
            index_items = self.get_index_items(language,category)

            # word in index ?
            for item in index_items:
                if(item["word"]==index_word):
                    return (item)
            
            return (None)


    def delete_index_item(self,item):

        table = DynamoDB.instance.Table(self.get_table("index"))

        try:
            response = table.delete_item(
                Key={
                    'id':item['id'],
                },
                
            )
        except Exception as e:
            print(e)
            return None
        else:
            return response

    def delete_product_word_item(self,item):

        table = DynamoDB.instance.Table(self.get_table("productWords"))

        try:
            response = table.delete_item(
                Key={
                    'id':item['id'],
                },
                
            )
        except Exception as e:
            print(e)
            return None
        else:
            return response

    def get_product_word_items_by_filename(self,filename):

        # Instantiate a table resource object without actually
        # creating a DynamoDB table. Note that the attributes of this table
        # are lazy-loaded: a request is not made nor are the attribute
        # values populated until the attributes
        # on the table resource are accessed or its load() method is called.
        table = DynamoDB.instance.Table(self.get_table("productWords")) # pylint: disable=no-member

        # Print out some data about the table.
        # This will cause a request to be made to DynamoDB and its attribute
        # values will be set based on the response.
        fe = Attr('filename').eq(filename) 

        pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo"
        # Expression Attribute Names for Projection Expression only.
        ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }
        #esk = None


        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items


    def get_product_word_items_by_category(self,category,language):

        table = DynamoDB.instance.Table(self.get_table("productWords")) # pylint: disable=no-member
        fe = Attr('category').eq(category) & Attr('language').eq(language)

        pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo"
        ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }

        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items

    def get_product_word_items_by_assignmentId(self,assignmentId):

        # Instantiate a table resource object without actually
        # creating a DynamoDB table. Note that the attributes of this table
        # are lazy-loaded: a request is not made nor are the attribute
        # values populated until the attributes
        # on the table resource are accessed or its load() method is called.
        table = DynamoDB.instance.Table(self.get_table("productWords")) # pylint: disable=no-member

        # Print out some data about the table.
        # This will cause a request to be made to DynamoDB and its attribute
        # values will be set based on the response.
        fe = Attr('assignmentId').eq(assignmentId) 

        pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo"
        # Expression Attribute Names for Projection Expression only.
        ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }
        #esk = None


        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items

    def delete_items(self,table_name,items):
        
        table = DynamoDB.instance.Table(table_name)

        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={
                    'id':item['id']
                })

    #TBD:  remove tranforms  to index tableetc..
    def update_transforms(self,items):
        table = DynamoDB.instance.Table(self.get_table("transforms")) # pylint: disable=no-member
        self.update_items(table,items)
        return

    def update_product_words(self,items):
        table = DynamoDB.instance.Table(self.get_table("productWords")) # pylint: disable=no-member
        self.update_items(table,items)
        return

    def update_transcript_errors(self,items):
        table = DynamoDB.instance.Table(self.get_table("transcriptErrors")) # pylint: disable=no-member
        self.update_items(table,items)
        return

    def get_transforms(self,language,category):

        table = DynamoDB.instance.Table(self.get_table("index")) # pylint: disable=no-member

        # Print out some data about the table.
        # This will cause a request to be made to DynamoDB and its attribute
        # values will be set based on the response.

        fe = Attr('category').eq(category) & Attr('language').eq(language)

        pe = "#cat, #indx,#trans,#lang,id,word"
        # Expression Attribute Names for Projection Expression only.
        ean = { "#cat": "category","#indx": "index", "#trans": "transform", "#lang": "language"}
        #esk = None

        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
            )

        raw_transforms =  response['Items']
        transforms = self.raw_transforms_to_dict(raw_transforms,language,category)
        return transforms

    def get_product_raw_word_items(self,raw_word,category):

        # Instantiate a table resource object without actually
        # creating a DynamoDB table. Note that the attributes of this table
        # are lazy-loaded: a request is not made nor are the attribute
        # values populated until the attributes
        # on the table resource are accessed or its load() method is called.
        table = DynamoDB.instance.Table(self.get_table("productWords")) # pylint: disable=no-member

        # Print out some data about the table.
        # This will cause a request to be made to DynamoDB and its attribute
        # values will be set based on the response.
        fe = Attr('category').eq(category) & Attr('rawWord').eq(raw_word)

        pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo,rawWord"
        # Expression Attribute Names for Projection Expression only.
        ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }
        #esk = None


        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
            )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items

    def get_product_word_items(self,word,language,category):

        # Instantiate a table resource object without actually
        # creating a DynamoDB table. Note that the attributes of this table
        # are lazy-loaded: a request is not made nor are the attribute
        # values populated until the attributes
        # on the table resource are accessed or its load() method is called.
        table = DynamoDB.instance.Table(self.get_table("productWords")) # pylint: disable=no-member

        # Print out some data about the table.
        # This will cause a request to be made to DynamoDB and its attribute
        # values will be set based on the response.
        fe = Attr('language').eq(language) & Attr('category').eq(category) & Attr('word').eq(word)

        pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo,rawWord"
        # Expression Attribute Names for Projection Expression only.
        ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }
        #esk = None


        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items

    def get_index_items(self,language,category):

        # Instantiate a table resource object without actually
        # creating a DynamoDB table. Note that the attributes of this table
        # are lazy-loaded: a request is not made nor are the attribute
        # values populated until the attributes
        # on the table resource are accessed or its load() method is called.
        table = DynamoDB.instance.Table(self.get_table("index")) # pylint: disable=no-member

        # Print out some data about the table.
        # This will cause a request to be made to DynamoDB and its attribute
        # values will be set based on the response.

        fe = Attr('category').eq(category) & Attr('language').eq(language)

        pe = "#cat, #indx,#trans,#lang,id,word"
        # Expression Attribute Names for Projection Expression only.
        ean = { "#cat": "category","#indx": "index", "#trans": "transform", "#lang": "language"}
        #esk = None


        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
            )

        items = response['Items']
        
        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items


    def get_index(self,language,category):

        # Instantiate a table resource object without actually
        # creating a DynamoDB table. Note that the attributes of this table
        # are lazy-loaded: a request is not made nor are the attribute
        # values populated until the attributes
        # on the table resource are accessed or its load() method is called.
        table = DynamoDB.instance.Table(self.get_table("index")) # pylint: disable=no-member

        # Print out some data about the table.
        # This will cause a request to be made to DynamoDB and its attribute
        # values will be set based on the response.

        fe = Attr('category').eq(category) & Attr('language').eq(language)

        pe = "#cat, #indx,#trans,#lang,id,word"
        # Expression Attribute Names for Projection Expression only.
        ean = { "#cat": "category","#indx": "index", "#trans": "transform", "#lang": "language"}
        #esk = None


        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
            )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        index = self.raw_index_to_dict(items,language,category)
        return index

    def raw_index_to_dict(self,raw_index,language,category):
        
        index_dict  = {}
        index_numbers_dict = {}
        for index_item in raw_index:

            # normelize all words when loading:
            key_orig = index_item['word']

            key = normelize_word(key_orig,language.lower(),category)
            val = index_item["id"]

            # same word with different indexes ?
            if(key in index_dict):
                raise AssertionError('('+category+ ' - ' +language + ') index is invalid, word exists twice with different indexes:',key+':'+str(index_dict[key])+ ' and  '+key_orig+':'+str(val))
            
            index_dict[key] = val

            # make additional numbers dict for hole validtion
            if("index" in index_item):
                index_numbers_dict[key] = index_item["index"]

        
        # hole exists?
        hole_index = self.find_index_hole(index_numbers_dict)
        if (not (hole_index == None)):
            raise AssertionError('('+category+ ' - ' +language + ') index is invalid, found hole at:',hole_index)

        index_dict["nextIndexVal"] = len(index_numbers_dict.values()) -1 
        return index_dict

    def find_index_hole(self,index_dict):
        
        indexes = list(index_dict.values())
        indexes.sort()
        for count_idx,index in enumerate(indexes,-1) :
            if not count_idx == index:
                return count_idx
                
        return None 

    def index_items_to_dict(self,items,language,category):
        
        index_dict  = {}
        for index_item in items:

            key_orig =  index_item['word']
            if("index" in index_item):
                val = index_item["index"]
            else:
                continue
            # normelize all words when loading:
            key = normelize_word(key_orig,language.lower(),category)
            if(key in index_dict):
                raise AssertionError('('+category+ ' - ' +language + ') index is invalid, word exists twice with different indexes:',key+':'+str(index_dict[key])+ ' and  '+key_orig+':'+str(val))
            
            index_dict[key] = val
        
        return index_dict

    def raw_transforms_to_dict(self,raw_transforms,language,category):

        transforms_dict  = {}
        for transforms_item in raw_transforms:
            key = transforms_item["word"]
            if("transform" in transforms_item):
                val = transforms_item["transform"]
            else:
                continue

            key = normelize_word(key,language,category)
            val = normelize_word(val,language,category)
            
            transforms_dict[key] = val
        
        return transforms_dict

    def get_pws_all(self):

        table = DynamoDB.instance.Table("ProductWords") # pylint: disable=no-member

        pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo"
        ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }

        response = table.scan(
        
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( 
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items

    def get_pws_items_no_end(self):

        table = DynamoDB.instance.Table("ProductWords") # pylint: disable=no-member
        fe = Attr('end').exists()
        pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo"
        ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }

        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items


    def get_pws_items_start_as_string(self):

        table = DynamoDB.instance.Table("ProductWords") # pylint: disable=no-member
        fe = Attr('start').attribute_type('S')
        pe = "#cat,#lang, #indx,#wrd,id,#strt,totalDuration,filename,assignmentId,#end,userInfo"
        ean = { "#cat": "category","#wrd":"word","#indx": "index","#strt":"start","#end":"end", "#lang": "language" }

        response = table.scan(
            FilterExpression=fe,
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )

        items = response['Items']

        while response.get('LastEvaluatedKey'):
            response = table.scan( FilterExpression=fe,
                                    ProjectionExpression=pe,
                                    ExpressionAttributeNames=ean,
                                    ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])        

        return items



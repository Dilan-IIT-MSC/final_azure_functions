# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(bp_story) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
import json
import pyodbc
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings

bp_story = func.Blueprint()

def format_date(date_obj):
    if date_obj:
        return date_obj.strftime('%Y-%m-%d')
    return None

def format_time(time_obj):
    if time_obj:
        return str(time_obj)
    return None

def format_user(user_data):
    return {
        "id": user_data[0],
        "firstName": user_data[1],
        "lastName": user_data[2]
    }

# Get Stories
@bp_story.route(route="stories", methods=["POST"])
def get_stories(req: func.HttpRequest) -> func.HttpResponse:
    try:
        try:
            req_body = req.get_json() if req.get_body() else {}
        except ValueError:
            req_body = {}
        
        user_id = req_body.get('user_id')
        category_id = req_body.get('category_id')
        order = req_body.get('order', 'ascending')
        
        if user_id is not None:
            try:
                user_id = int(user_id)
            except ValueError:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "Invalid user ID format"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
                
        if category_id is not None:
            try:
                category_id = int(category_id)
            except ValueError:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "Invalid category ID format"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        base_query = '''
        SELECT 
            s.id,
            s.title,
            s.created,
            s.duration,
            u.id AS user_id,
            u.firstName,
            u.lastName,
            (SELECT COUNT(*) FROM story_has_likes WHERE story_id = s.id AND status = 1) AS like_count
        FROM 
            story s
        INNER JOIN 
            "user" u ON s.user_id = u.id
        WHERE 
            s.status = 1
        '''
        
        params = []
        
        if user_id is not None:
            base_query += ' AND s.user_id = ?'
            params.append(user_id)
            
        if category_id is not None:
            base_query = base_query.replace('WHERE s.status = 1', 
                                           'INNER JOIN story_has_categories shc ON s.id = shc.story_id WHERE s.status = 1 AND shc.category_id = ?')
            params.append(category_id)
        
        order_direction = "DESC" if order.lower() == "descending" else "ASC"
        base_query += f' ORDER BY s.created {order_direction}'
        
        cursor.execute(base_query, params)
        stories = cursor.fetchall()
        result = []
        
        for story in stories:
            story_id = story[0]
            category_query = '''
            SELECT 
                c.id,
                c.name,
                c.description
            FROM 
                category c
            INNER JOIN 
                story_has_categories shc ON c.id = shc.category_id
            WHERE 
                shc.story_id = ? AND c.status = 1
            '''
            
            cursor.execute(category_query, story_id)
            categories = cursor.fetchall()
            
            category_list = []
            for category in categories:
                category_list.append({
                    "id": category[0],
                    "name": category[1],
                    "description": category[2]
                })
            
            story_obj = {
                "id": story[0],
                "title": story[1],
                "created": format_date(story[2]),
                "duration": format_time(story[3]),
                "author": {
                    "id": story[4],
                    "firstName": story[5],
                    "lastName": story[6]
                },
                "categories": category_list,
                "likeCount": story[7]
            }
            
            result.append(story_obj)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "Stories retrieved successfully",
                "stories": result
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while retrieving stories: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()
                      
@bp_story.route(route="story/{id}", methods=["GET"])
def get_story_detail(req: func.HttpRequest) -> func.HttpResponse:
    try:
        story_id = req.route_params.get('id')
        try:
            story_id = int(story_id)
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid story ID format"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        story_query = '''
        SELECT 
            s.id,
            s.title,
            s.story_url,
            s.gen_audio_url,
            s.created,
            s.duration,
            s.listen_count,
            s.status,
            u.id AS user_id,
            u.firstName,
            u.lastName,
            u.bday,
            (SELECT COUNT(*) FROM story_has_likes WHERE story_id = s.id AND status = 1) AS like_count
        FROM 
            story s
        INNER JOIN 
            "user" u ON s.user_id = u.id
        WHERE 
            s.id = ? AND s.status = 1
        '''
        
        cursor.execute(story_query, story_id)
        story_data = cursor.fetchone()
        
        if not story_data:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Story not found or inactive"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        # Get categories for this story
        category_query = '''
        SELECT 
            c.id,
            c.name,
            c.description,
            c.icon
        FROM 
            category c
        INNER JOIN 
            story_has_categories shc ON c.id = shc.category_id
        WHERE 
            shc.story_id = ? AND c.status = 1
        '''
        
        cursor.execute(category_query, story_id)
        categories = cursor.fetchall()
        
        category_list = []
        for category in categories:
            category_list.append({
                "id": category[0],
                "name": category[1],
                "description": category[2],
                "icon": category[3]
            })
        
        timeline_query = '''
        SELECT 
            id,
            time,
            color
        FROM 
            timeline_color
        WHERE 
            story_id = ?
        ORDER BY 
            time ASC
        '''
        
        cursor.execute(timeline_query, story_id)
        timeline_colors = cursor.fetchall()
        
        timeline_list = []
        for timeline in timeline_colors:
            timeline_list.append({
                "id": timeline[0],
                "time": format_time(timeline[1]),
                "color": timeline[2]
            })
        
        # Get likes details for this story
        likes_query = '''
        SELECT 
            shl.id,
            shl.user_id,
            u.firstName,
            u.lastName,
            shl.updated
        FROM 
            story_has_likes shl
        INNER JOIN 
            "user" u ON shl.user_id = u.id
        WHERE 
            shl.story_id = ? AND shl.status = 1
        ORDER BY 
            shl.updated DESC
        '''
        
        cursor.execute(likes_query, story_id)
        likes = cursor.fetchall()
        
        likes_list = []
        for like in likes:
            likes_list.append({
                "id": like[0],
                "user": {
                    "id": like[1],
                    "firstName": like[2],
                    "lastName": like[3]
                },
                "updated": format_date(like[4])
            })
        
        # Get recent listeners
        listeners_query = '''
        SELECT 
            uhls.id,
            uhls.user_id,
            u.firstName,
            u.lastName,
            uhls.listen_time,
            uhls.end_duration
        FROM 
            user_has_listen_stories uhls
        INNER JOIN 
            "user" u ON uhls.user_id = u.id
        WHERE 
            uhls.story_id = ?
        ORDER BY 
            uhls.listen_time DESC
        LIMIT 10
        '''
        
        try:
            cursor.execute(listeners_query, story_id)
        except:
            listeners_query = listeners_query.replace("LIMIT 10", "")
            listeners_query = listeners_query.replace("SELECT", "SELECT TOP 10")
            cursor.execute(listeners_query, story_id)
        
        listeners = cursor.fetchall()
        
        listeners_list = []
        for listener in listeners:
            listeners_list.append({
                "id": listener[0],
                "user": {
                    "id": listener[1],
                    "firstName": listener[2],
                    "lastName": listener[3]
                },
                "listenTime": format_date(listener[4]),
                "endDuration": format_time(listener[5])
            })
        
        story_obj = {
            "id": story_data[0],
            "title": story_data[1],
            "storyUrl": story_data[2],
            "genAudioUrl": story_data[3],
            "created": format_date(story_data[4]),
            "duration": format_time(story_data[5]),
            "listenCount": story_data[6],
            "status": story_data[7],
            "author": {
                "id": story_data[8],
                "firstName": story_data[9],
                "lastName": story_data[10],
                "birthDate": format_date(story_data[11])
            },
            "likeCount": story_data[12],
            "categories": category_list,
            "timelineColors": timeline_list,
            "likes": likes_list,
            "recentListeners": listeners_list
        }
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "Story details retrieved successfully",
                "story": story_obj
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while retrieving story details: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()

@bp_story.route(route="story/like", methods=["POST"])
def update_story_like(req: func.HttpRequest) -> func.HttpResponse:
    try:
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid JSON in request body"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        if 'story_id' not in req_body:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Missing required field: story_id"
                }),
                mimetype="application/json",
                status_code=200
            )
            
        if 'user_id' not in req_body:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Missing required field: user_id"
                }),
                mimetype="application/json",
                status_code=200
            )
            
        if 'action' not in req_body:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Missing required field: action (increase or decrease)"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        story_id = req_body['story_id']
        user_id = req_body['user_id']
        action = req_body['action'].lower()
        
        if action not in ['increase', 'decrease']:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid action value. Must be 'increase' or 'decrease'"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        try:
            story_id = int(story_id)
            user_id = int(user_id)
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid ID format. Both story_id and user_id must be integers"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM story WHERE id = ? AND status = 1', story_id)
        story = cursor.fetchone()
        
        if not story:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Story not found or inactive"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        cursor.execute('SELECT id FROM "user" WHERE id = ? AND status = 1', user_id)
        user = cursor.fetchone()
        
        if not user:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User not found or inactive"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        cursor.execute('SELECT id, status FROM story_has_likes WHERE user_id = ? AND story_id = ?', 
                      user_id, story_id)
        like_record = cursor.fetchone()
        
        current_time = datetime.now()
        
        if action == 'increase':
            if like_record:
                if like_record[1] == 0:
                    cursor.execute('UPDATE story_has_likes SET status = 1, updated = ? WHERE id = ?', 
                                  current_time, like_record[0])
                else:
                    return func.HttpResponse(
                        body=json.dumps({
                            "status": True,
                            "message": "Story already liked by this user",
                            "like_id": like_record[0]
                        }),
                        mimetype="application/json",
                        status_code=200
                    )
            else:
                cursor.execute('INSERT INTO story_has_likes (user_id, story_id, updated, status) VALUES (?, ?, ?, 1)',
                             user_id, story_id, current_time)
                
                cursor.execute("SELECT @@IDENTITY AS id")
                new_like_id = cursor.fetchone()[0]
        
        elif action == 'decrease':
            if like_record and like_record[1] == 1:
                cursor.execute('UPDATE story_has_likes SET status = 0, updated = ? WHERE id = ?', 
                              current_time, like_record[0])
            else:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": True,
                        "message": "No active like found to remove",
                    }),
                    mimetype="application/json",
                    status_code=200
                )
        
        conn.commit()
        cursor.execute('SELECT COUNT(*) FROM story_has_likes WHERE story_id = ? AND status = 1', story_id)
        updated_count = cursor.fetchone()[0]
        
        if action == 'increase':
            response_message = "Story liked successfully"
            response_data = {
                "status": True,
                "message": response_message,
                "like_id": new_like_id if 'new_like_id' in locals() else like_record[0],
                "likeCount": updated_count
            }
        else: 
            response_message = "Story unliked successfully"
            response_data = {
                "status": True,
                "message": response_message,
                "likeCount": updated_count
            }
        
        return func.HttpResponse(
            body=json.dumps(response_data),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Exception while updating story like: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()
            
@bp_story.route(route="story/upload", methods=["POST"])
def upload_story(req: func.HttpRequest) -> func.HttpResponse:
    try:
        form = req.form
        user_id = form.get('user_id')
        title = form.get('title')
        categories = json.loads(form.get('categories', '[]'))
        
        if not user_id or not title:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Missing required fields: user_id, title"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        try:
            user_id = int(user_id)
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid user ID format"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        if not isinstance(categories, list):
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Categories must be an array"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        if len(categories) > 3:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Maximum 3 categories allowed"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        if not req.files or 'audio' not in req.files:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "No audio file provided"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        audio_file = req.files['audio']
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        cursor.execute('SELECT id FROM "user" WHERE id = ? AND status = 1', user_id)
        user = cursor.fetchone()
        if not user:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User not found or inactive"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        if categories:
            placeholders = ', '.join(['?' for _ in categories])
            query = f'SELECT id FROM "category" WHERE id IN ({placeholders}) AND status = 1'
            cursor.execute(query, *categories)
            found_categories = cursor.fetchall()
            if len(found_categories) != len(categories):
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "One or more categories not found or inactive"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
        
        duration = "00:10:00"
        now = datetime.now()
        
        cursor.execute('''
            INSERT INTO "story" (user_id, title, created, duration, listen_count, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', user_id, title, now, duration, 0, 1)
        
        cursor.execute("SELECT @@IDENTITY AS id")
        story_id = cursor.fetchone()[0]
        
        for category_id in categories:
            cursor.execute('''
                INSERT INTO "story_has_categories" (story_id, category_id)
                VALUES (?, ?)
            ''', story_id, category_id)
        
        filename = f"{user_id}/{story_id}/{now.strftime('%Y%m%d%H%M%S')}.aac"
        
        try:
            connection_string = os.environ["AzureBlobStorageConnectionString"]
            container_name = os.environ["AudioStorageContainerName"]

            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(filename)
            content_settings = ContentSettings(content_type="audio/aac")
            
            blob_client.upload_blob(
                audio_file,
                content_settings=content_settings,
                overwrite=True
            )
            
            story_url = blob_client.url
            
            cursor.execute('''
                UPDATE "story" 
                SET story_url = ?
                WHERE id = ?
            ''', story_url, story_id)
            
            conn.commit()
            
            return func.HttpResponse(
                body=json.dumps({
                    "status": True,
                    "message": "Story uploaded successfully",
                    "story": {
                        "id": story_id,
                        "title": title,
                        "user_id": user_id,
                        "story_url": story_url,
                        "created": now.strftime('%Y-%m-%d %H:%M:%S'),
                        "duration": duration,
                        "categories": categories
                    }
                }, default=str),
                mimetype="application/json",
                status_code=200
            )
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Blob storage error: {str(e)}")
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": f"Error uploading to blob storage: {str(e)}"
                }),
                mimetype="application/json",
                status_code=200
            )
    
    except Exception as e:
        logging.error(f"Exception while uploading story: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()
            
@bp_story.route(route="stories/category/{id}", methods=["GET"])
def get_stories_by_category(req: func.HttpRequest) -> func.HttpResponse:
    try:
        category_id = req.route_params.get('id')
        try:
            category_id = int(category_id)
        except ValueError:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Invalid category ID format"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        order = req.params.get('order', 'descending')
        limit = req.params.get('limit')
        
        if limit:
            try:
                limit = int(limit)
                if limit <= 0:
                    return func.HttpResponse(
                        body=json.dumps({
                            "status": False,
                            "message": "Limit must be a positive integer"
                        }),
                        mimetype="application/json",
                        status_code=200
                    )
            except ValueError:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": "Invalid limit format"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
        
        connection_string = os.environ["AzureBlobStorageConnectionString"]
        container_name = os.environ.get("StoryImagesContainerName", "storyImages")
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        cursor.execute('SELECT id, name FROM "category" WHERE id = ? AND status = 1', category_id)
        category = cursor.fetchone()
        
        if not category:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Category not found or inactive"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        base_query = '''
        SELECT 
            s.id,
            s.title,
            s.created,
            s.duration,
            s.listen_count,
            u.id AS user_id,
            u.firstName,
            u.lastName,
            (SELECT COUNT(*) FROM story_has_likes WHERE story_id = s.id AND status = 1) AS like_count
        FROM 
            story s
        INNER JOIN 
            "user" u ON s.user_id = u.id
        INNER JOIN 
            story_has_categories shc ON s.id = shc.story_id
        WHERE 
            s.status = 1
            AND shc.category_id = ?
        '''
        
        params = [category_id]
        
        order_direction = "DESC" if order.lower() == "descending" else "ASC"
        base_query += f' ORDER BY s.created {order_direction}'
        
        if limit:
            try:
                base_query += f" FETCH FIRST {limit} ROWS ONLY"
            except:
                base_query = f"SELECT TOP {limit} " + base_query[7:]
        
        cursor.execute(base_query, params)
        stories = cursor.fetchall()
        result = []
        
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        for story in stories:
            story_id = story[0]
            category_query = '''
            SELECT 
                c.id,
                c.name,
                c.description
            FROM 
                category c
            INNER JOIN 
                story_has_categories shc ON c.id = shc.category_id
            WHERE 
                shc.story_id = ? AND c.status = 1
            '''
            
            cursor.execute(category_query, story_id)
            categories = cursor.fetchall()
            
            category_list = []
            for cat in categories:
                category_list.append({
                    "id": cat[0],
                    "name": cat[1],
                    "description": cat[2]
                })
            
            thumbnail_blob_name = f"{story_id}_1.jpeg"
            thumbnail_blob_client = container_client.get_blob_client(thumbnail_blob_name)
            thumbnail_url = thumbnail_blob_client.url
            
            created_date = None
            if story[2]:
                if hasattr(story[2], 'strftime'):
                    created_date = story[2].strftime('%Y-%m-%d')
                else:
                    created_date = str(story[2])
            
            duration_time = None
            if story[3]:
                if hasattr(story[3], 'strftime'):
                    duration_time = story[3].strftime('%H:%M:%S')
                else:
                    duration_time = str(story[3])
            
            story_obj = {
                "id": story[0],
                "title": story[1],
                "thumbnailUrl": thumbnail_url,
                "created": created_date,
                "duration": duration_time,
                "listenCount": story[4],
                "author": {
                    "id": story[5],
                    "firstName": story[6],
                    "lastName": story[7]
                },
                "categories": category_list,
                "likeCount": story[8]
            }
            
            result.append(story_obj)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "Stories retrieved successfully",
                "category": {
                    "id": category[0],
                    "name": category[1]
                },
                "stories": result,
                "count": len(result)
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while retrieving stories by category: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False,
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=200
        )
    finally:
        if 'conn' in locals():
            conn.close()
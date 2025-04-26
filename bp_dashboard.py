# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(bp_dashboard) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
import json
import pyodbc
import os
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

bp_dashboard = func.Blueprint()

def format_date(date_obj):
    if date_obj:
        return date_obj.strftime('%Y-%m-%d')
    return None

def format_time(time_obj):
    if time_obj:
        return str(time_obj)
    return None

def format_user(user_data):
    if not user_data:
        return None
    return {
        "id": user_data[0],
        "firstName": user_data[1],
        "lastName": user_data[2] if len(user_data) > 2 else None
    }

@bp_dashboard.route(route="dashboard", methods=["POST"])
def get_dashboard_data(req: func.HttpRequest) -> func.HttpResponse:
    try:
        try:
            req_body = req.get_json() if req.get_body() else {}
        except ValueError:
            req_body = {}

        user_id = req_body.get('user_id')
        
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
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        
        trending_stories = get_trending_stories(cursor, 5)
        if not trending_stories:
            trending_stories = get_most_recent_stories(cursor, 5)
        
        recently_listened = []
        if user_id:
            recently_listened = get_recently_listened_stories(cursor, user_id, 2)
            if not recently_listened:
                recently_listened = get_recommended_stories(cursor, user_id, 2)
        
        trending_categories = get_trending_categories(cursor, 4)
        if not trending_categories:
            trending_categories = get_most_popular_categories(cursor, 4)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "Dashboard data retrieved successfully",
                "dashboard": {
                    "trendingStories": trending_stories,
                    "recentlyListened": recently_listened,
                    "trendingCategories": trending_categories
                }
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"Exception while retrieving dashboard data: {str(e)}")
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

def get_trending_stories(cursor, limit=5):
    """Get trending stories based on recent listens and likes."""
    cutoff_date = datetime.now() - timedelta(days=14)
    
    try:
        trending_query = """
        SELECT 
            s.id, 
            s.title,
            s.story_url,
            s.gen_audio_url,
            s.created,
            s.duration,
            COUNT(DISTINCT uhls.id) * 2 AS listen_score,
            COUNT(DISTINCT shl.id) * 4 AS like_score,
            (COUNT(DISTINCT uhls.id) * 2 + COUNT(DISTINCT shl.id) * 4) AS total_score,
            u.id AS user_id,
            u.firstName,
            u.lastName
        FROM 
            story s
        JOIN 
            "user" u ON s.user_id = u.id
        LEFT JOIN 
            user_has_listen_stories uhls ON s.id = uhls.story_id 
            AND uhls.listen_time > ?
        LEFT JOIN 
            story_has_likes shl ON s.id = shl.story_id 
            AND shl.updated > ?
            AND shl.status = 1
        WHERE 
            s.status = 1
        GROUP BY 
            s.id, s.title, s.story_url, s.gen_audio_url, s.created, s.duration,
            u.id, u.firstName, u.lastName
        HAVING
            (COUNT(DISTINCT uhls.id) * 2 + COUNT(DISTINCT shl.id) * 4) > 0
        ORDER BY 
            total_score DESC
        """
        
        trending_query = f"SELECT TOP {limit} " + trending_query.split("SELECT ")[1]
        cursor.execute(trending_query, cutoff_date, cutoff_date)
        stories = cursor.fetchall()
        
        result = []
        for story in stories:
            cursor.execute("""
                SELECT 
                    c.id, c.name, c.description, c.icon
                FROM 
                    category c
                JOIN 
                    story_has_categories shc ON c.id = shc.category_id
                WHERE 
                    shc.story_id = ? AND c.status = 1
            """, story[0])
            
            categories = cursor.fetchall()
            category_list = []
            
            for category in categories:
                category_list.append({
                    "id": category[0],
                    "name": category[1],
                    "description": category[2],
                    "icon": category[3]
                })
            
            story_obj = {
                "id": story[0],
                "title": story[1],
                "created": format_date(story[4]),
                "duration": format_time(story[5]),
                "author": {
                    "id": story[9],
                    "firstName": story[10],
                    "lastName": story[11]
                },
                "categories": category_list
            }
            
            result.append(story_obj)
            
        return result
        
    except Exception as e:
        logging.error(f"Error in get_trending_stories: {str(e)}")
        return []

def get_most_recent_stories(cursor, limit=5):
    """Fallback method to get most recent stories when trending data is not available."""
    try:
        query = """
        SELECT 
            s.id, 
            s.title,
            s.story_url,
            s.gen_audio_url,
            s.created,
            s.duration,
            s.listen_count,
            u.id AS user_id,
            u.firstName,
            u.lastName
        FROM 
            story s
        JOIN 
            "user" u ON s.user_id = u.id
        WHERE 
            s.status = 1
        ORDER BY 
            s.created DESC
        """
        
        query = f"SELECT TOP {limit} " + query.split("SELECT ")[1]
        cursor.execute(query)
        stories = cursor.fetchall()
        
        result = []
        for story in stories:
            cursor.execute("""
                SELECT 
                    c.id, c.name, c.description, c.icon
                FROM 
                    category c
                JOIN 
                    story_has_categories shc ON c.id = shc.category_id
                WHERE 
                    shc.story_id = ? AND c.status = 1
            """, story[0])
            
            categories = cursor.fetchall()
            category_list = []
            
            for category in categories:
                category_list.append({
                    "id": category[0],
                    "name": category[1],
                    "description": category[2],
                    "icon": category[3]
                })
            
            story_obj = {
                "id": story[0],
                "title": story[1],
                "created": format_date(story[4]),
                "duration": format_time(story[5]),
                "listenCount": story[6],
                "author": {
                    "id": story[7],
                    "firstName": story[8],
                    "lastName": story[9]
                },
                "categories": category_list,
            }
            
            result.append(story_obj)
            
        return result
        
    except Exception as e:
        logging.error(f"Error in get_most_recent_stories: {str(e)}")
        return []

def get_recently_listened_stories(cursor, user_id, limit=2):
    """Get recently listened stories for a specific user."""
    try:
        query = """
        SELECT 
            s.id, 
            s.title,
            s.story_url,
            s.duration,
            MAX(uhls.listen_time) as last_listen_time,
            uhls.end_duration,
            u.id AS user_id,
            u.firstName,
            u.lastName
        FROM 
            user_has_listen_stories uhls
        JOIN 
            story s ON uhls.story_id = s.id
        JOIN 
            "user" u ON s.user_id = u.id
        WHERE 
            uhls.user_id = ? 
            AND s.status = 1
        GROUP BY 
            s.id, s.title, s.story_url, s.duration, uhls.end_duration,
            u.id, u.firstName, u.lastName
        ORDER BY 
            last_listen_time DESC
        """
        
        query = f"SELECT TOP {limit} " + query.split("SELECT ")[1]
        cursor.execute(query, user_id)
        stories = cursor.fetchall()
        
        result = []
        for story in stories:
            total_duration = story[3] 
            listened_duration = story[5]
            
            cursor.execute("""
                SELECT 
                    c.id, c.name, c.description, c.icon
                FROM 
                    category c
                JOIN 
                    story_has_categories shc ON c.id = shc.category_id
                WHERE 
                    shc.story_id = ? AND c.status = 1
            """, story[0])
            
            categories = cursor.fetchall()
            category_list = []
            
            for category in categories:
                category_list.append({
                    "id": category[0],
                    "name": category[1],
                    "description": category[2],
                    "icon": category[3]
                })
            
            story_obj = {
                "id": story[0],
                "title": story[1],
                "storyUrl": story[2],
                "duration": format_time(story[3]),
                "lastListenTime": format_date(story[4]),
                "listenedDuration": format_time(story[5]),
                "author": {
                    "id": story[6],
                    "firstName": story[7],
                    "lastName": story[8]
                },
                "categories": category_list
            }
            
            result.append(story_obj)
            
        return result
        
    except Exception as e:
        logging.error(f"Error in get_recently_listened_stories: {str(e)}")
        return []

def get_recommended_stories(cursor, user_id, limit=2):
    """Fallback method to get recommended stories based on user preferences when no recently listened stories exist."""
    try:
        cursor.execute("""
            SELECT category_id 
            FROM user_preferred_categories 
            WHERE user_id = ? AND status = 1
        """, user_id)
        
        preferred_categories = cursor.fetchall()
        
        if preferred_categories:
            placeholders = ','.join(['?' for _ in preferred_categories])
            category_ids = [cat[0] for cat in preferred_categories]
            
            query = f"""
            SELECT 
                s.id, 
                s.title,
                s.story_url,
                s.duration,
                s.created,
                u.id AS user_id,
                u.firstName,
                u.lastName
            FROM 
                story s
            JOIN 
                "user" u ON s.user_id = u.id
            JOIN 
                story_has_categories shc ON s.id = shc.story_id
            WHERE 
                s.status = 1
                AND shc.category_id IN ({placeholders})
                AND s.id NOT IN (
                    SELECT story_id FROM user_has_listen_stories WHERE user_id = ?
                )
            ORDER BY 
                s.created DESC
            """
            
            query = f"SELECT TOP {limit} " + query.split("SELECT ")[1]
            
            params = category_ids + [user_id]
            cursor.execute(query, params)
        else:
            query = """
            SELECT 
                s.id, 
                s.title,
                s.story_url,
                s.duration,
                s.created,
                u.id AS user_id,
                u.firstName,
                u.lastName
            FROM 
                story s
            JOIN 
                "user" u ON s.user_id = u.id
            WHERE 
                s.status = 1
                AND s.id NOT IN (
                    SELECT story_id FROM user_has_listen_stories WHERE user_id = ?
                )
            ORDER BY 
                s.listen_count DESC
            """
            
            query = f"SELECT TOP {limit} " + query.split("SELECT ")[1]
            
            cursor.execute(query, user_id)
        
        stories = cursor.fetchall()
        
        result = []
        for story in stories:
            cursor.execute("""
                SELECT 
                    c.id, c.name, c.description, c.icon
                FROM 
                    category c
                JOIN 
                    story_has_categories shc ON c.id = shc.category_id
                WHERE 
                    shc.story_id = ? AND c.status = 1
            """, story[0])
            
            categories = cursor.fetchall()
            category_list = []
            
            for category in categories:
                category_list.append({
                    "id": category[0],
                    "name": category[1],
                    "description": category[2],
                    "icon": category[3]
                })
            
            story_obj = {
                "id": story[0],
                "title": story[1],
                "storyUrl": story[2],
                "duration": format_time(story[3]),
                "created": format_date(story[4]),
                "author": {
                    "id": story[5],
                    "firstName": story[6],
                    "lastName": story[7]
                },
                "categories": category_list,
                "isRecommended": True
            }
            
            result.append(story_obj)
        
        return result
        
    except Exception as e:
        logging.error(f"Error in get_recommended_stories: {str(e)}")
        return []

def get_trending_categories(cursor, limit=4):
    """Get trending categories based on recent listens and likes."""
    # Define the cutoff date for recent activity (21 days ago)
    cutoff_date = datetime.now() - timedelta(days=21)
    
    try:
        # Query to get trending categories
        query = """
        SELECT 
            c.id,
            c.name,
            c.description,
            c.icon,
            COUNT(DISTINCT uhls.id) * 2 AS listen_score,
            COUNT(DISTINCT shl.id) * 4 AS like_score,
            COUNT(DISTINCT s.id) AS story_count,
            (COUNT(DISTINCT uhls.id) * 2 + COUNT(DISTINCT shl.id) * 4) AS total_score
        FROM 
            category c
        JOIN 
            story_has_categories shc ON c.id = shc.category_id
        JOIN 
            story s ON shc.story_id = s.id
        LEFT JOIN 
            user_has_listen_stories uhls ON s.id = uhls.story_id 
            AND uhls.listen_time > ?
        LEFT JOIN 
            story_has_likes shl ON s.id = shl.story_id 
            AND shl.updated > ?
            AND shl.status = 1
        WHERE 
            c.status = 1
            AND s.status = 1
        GROUP BY 
            c.id, c.name, c.description, c.icon
        HAVING
            (COUNT(DISTINCT uhls.id) * 2 + COUNT(DISTINCT shl.id) * 4) > 0
        ORDER BY 
            total_score DESC
        """
        
        # For SQL Server, modify query to use TOP instead of LIMIT
        query = f"SELECT TOP {limit} " + query.split("SELECT ")[1]
        
        cursor.execute(query, cutoff_date, cutoff_date)
        categories = cursor.fetchall()
        
        # Get Azure Blob Storage connection for category images
        connection_string = os.environ["AzureBlobStorageConnectionString"]
        container_name = os.environ.get("CategoryImagesContainerName", "categories")
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Process and format the results
        result = []
        for category in categories:
            # Get a sample story for this category
            cursor.execute("""
                SELECT TOP 1
                    s.id, s.title
                FROM 
                    story s
                JOIN 
                    story_has_categories shc ON s.id = shc.story_id
                WHERE 
                    shc.category_id = ? 
                    AND s.status = 1
                ORDER BY
                    s.created DESC
            """, category[0])
            
            sample_story = cursor.fetchone()
            
            # Get category image URL
            cat_id = category[0]
            image_filename = f"{cat_id}.jpeg"
            blob_client = container_client.get_blob_client(image_filename)
            
            # Format the category data
            category_obj = {
                "id": category[0],
                "name": category[1],
                "description": category[2],
                "icon": category[3],
                "imageURL": blob_client.url,
                "storyCount": int(category[6]) if category[6] else 0,
            }
            
            result.append(category_obj)
            
        return result
    
    except Exception as e:
        logging.error(f"Error in get_trending_categories: {str(e)}")
        return []

def get_most_popular_categories(cursor, limit=4):
    """Fallback method to get most popular categories when trending data is not available."""
    try:
        # Query to get categories with the most stories
        query = """
        SELECT 
            c.id,
            c.name,
            c.description,
            c.icon,
            COUNT(DISTINCT shc.story_id) AS story_count
        FROM 
            category c
        JOIN 
            story_has_categories shc ON c.id = shc.category_id
        JOIN 
            story s ON shc.story_id = s.id
        WHERE 
            c.status = 1
            AND s.status = 1
        GROUP BY 
            c.id, c.name, c.description, c.icon
        ORDER BY 
            story_count DESC
        """
        
        # For SQL Server, modify query to use TOP instead of LIMIT
        query = f"SELECT TOP {limit} " + query.split("SELECT ")[1]
        
        cursor.execute(query)
        categories = cursor.fetchall()
        
        # If no categories with stories, get all active categories
        if not categories:
            query = """
            SELECT 
                c.id,
                c.name,
                c.description,
                c.icon
            FROM 
                category c
            WHERE 
                c.status = 1
            ORDER BY 
                c.name
            """
            
            # For SQL Server, modify query to use TOP instead of LIMIT
            query = f"SELECT TOP {limit} " + query.split("SELECT ")[1]
            
            cursor.execute(query)
            categories = cursor.fetchall()
        
        # Get Azure Blob Storage connection for category images
        connection_string = os.environ["AzureBlobStorageConnectionString"]
        container_name = os.environ.get("CategoryImagesContainerName", "categories")
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Process and format the results
        result = []
        for category in categories:
            # Get a sample story for this category
            cursor.execute("""
                SELECT TOP 1
                    s.id, s.title
                FROM 
                    story s
                JOIN 
                    story_has_categories shc ON s.id = shc.story_id
                WHERE 
                    shc.category_id = ? 
                    AND s.status = 1
                ORDER BY
                    s.created DESC
            """, category[0])
            
            sample_story = cursor.fetchone()
            
            # Get category image URL
            cat_id = category[0]
            image_filename = f"{cat_id}.jpeg"
            blob_client = container_client.get_blob_client(image_filename)
            
            # Format the category data
            category_obj = {
                "id": category[0],
                "name": category[1],
                "description": category[2],
                "icon": category[3],
                "imageURL": blob_client.url,
                "storyCount": int(category[4]) if len(category) > 4 and category[4] else 0
            }
            
            result.append(category_obj)
            
        return result
        
    except Exception as e:
        logging.error(f"Error in get_most_popular_categories: {str(e)}")
        return []
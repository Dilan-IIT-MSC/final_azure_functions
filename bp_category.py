# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(bp_category) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
import json
import pyodbc
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

bp_category = func.Blueprint()

@bp_category.route(route="user/{id}/categories", methods=["GET"])
def get_user_categories(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('id')
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
        cursor.execute('SELECT id FROM "user" WHERE id = ?', user_id)
        user = cursor.fetchone()
        
        if not user:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User not found"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        query = """
            SELECT c.id, c.name, c.description 
            FROM "category" c
            JOIN "user_preferred_categories" upc ON c.id = upc.category_id
            WHERE upc.user_id = ? AND upc.status = 1 AND c.status = 1
            ORDER BY upc.created DESC
        """
        
        cursor.execute(query, user_id)
        categories = cursor.fetchall()
        
        categories_data = []
        if categories:
            column_names = [column[0] for column in cursor.description]
            for category in categories:
                category_dict = {}
                for i, column in enumerate(column_names):
                    category_dict[column] = category[i]
                categories_data.append(category_dict)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "User categories fetched successfully",
                "categories": categories_data,
                "count": len(categories_data)
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while getting user categories: {str(e)}")
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

@bp_category.route(route="user/{id}/categories", methods=["PUT"])
def update_user_categories(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.route_params.get('id')
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
        
        if 'categories' not in req_body or not isinstance(req_body['categories'], list):
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "Request must include 'categories' as an array"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        category_ids = req_body['categories']
        
        if len(category_ids) > 3:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User can have a maximum of 3 preferred categories"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM "user" WHERE id = ?', user_id)
        user = cursor.fetchone()
        
        if not user:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False,
                    "message": "User not found"
                }),
                mimetype="application/json",
                status_code=200
            )
        
        if category_ids:
            placeholders = ','.join(['?' for _ in category_ids])
            query = f'SELECT id FROM "category" WHERE id IN ({placeholders}) AND status = 1'
            cursor.execute(query, *category_ids)
            valid_categories = [row[0] for row in cursor.fetchall()]
            
            invalid_categories = [cat_id for cat_id in category_ids if cat_id not in valid_categories]
            if invalid_categories:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False,
                        "message": f"Invalid or inactive category IDs: {invalid_categories}"
                    }),
                    mimetype="application/json",
                    status_code=200
                )
        
        conn.autocommit = False
        
        try:
            cursor.execute(
                'UPDATE "user_preferred_categories" SET status = 0 WHERE user_id = ?',
                user_id
            )
            
            final_categories = []
            for category_id in category_ids:
                cursor.execute(
                    'SELECT id FROM "user_preferred_categories" WHERE user_id = ? AND category_id = ?',
                    user_id, category_id
                )
                existing = cursor.fetchone()
                
                if existing:
                    cursor.execute(
                        'UPDATE "user_preferred_categories" SET status = 1, created = GETDATE() WHERE id = ?',
                        existing[0]
                    )
                else:
                    cursor.execute(
                        'INSERT INTO "user_preferred_categories" (user_id, category_id, created, status) VALUES (?, ?, GETDATE(), 1)',
                        user_id, category_id
                    )
                
                cursor.execute(
                    'SELECT id, name, description FROM "category" WHERE id = ?',
                    category_id
                )
                category = cursor.fetchone()
                
                if category:
                    category_dict = {
                        'id': category[0],
                        'name': category[1],
                        'description': category[2]
                    }
                    final_categories.append(category_dict)
            
            conn.commit()
            
            return func.HttpResponse(
                body=json.dumps({
                    "status": True,
                    "message": "User categories updated successfully",
                    "categories": final_categories,
                    "count": len(final_categories)
                }, default=str),
                mimetype="application/json",
                status_code=200
            )
        except Exception as e:
            conn.rollback()
            raise e
            
    except Exception as e:
        logging.error(f"Exception while updating user categories: {str(e)}")
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
            if not conn.autocommit:
                conn.autocommit = True
            conn.close()

@bp_category.route(route="categories", methods=["GET"])
def get_categories(req: func.HttpRequest) -> func.HttpResponse:
    try:
        conn = pyodbc.connect(os.environ["SqlConnectionString"])
        cursor = conn.cursor()
        params = []
        query = 'SELECT * FROM "category"'
        
        query += " WHERE status = 1"
        query += " ORDER BY name ASC"
        
        cursor.execute(query, params)
        categories = cursor.fetchall()
        
        connection_string = os.environ["AzureBlobStorageConnectionString"]
        container_name = os.environ.get("CategoryImagesContainerName", "categories")
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        categories_data = []
        if categories:
            column_names = [column[0] for column in cursor.description]
            for category in categories:
                category_dict = {}
                for i, column in enumerate(column_names):
                    category_dict[column] = category[i]
                
                cat_id = category_dict["id"]
                image_filename = f"{cat_id}.jpeg"
                blob_client = container_client.get_blob_client(image_filename)
                category_dict["imageURL"] = blob_client.url
                
                categories_data.append(category_dict)
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "Categories fetched successfully",
                "categories": categories_data,
                "count": len(categories_data)
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Exception while getting categories: {str(e)}")
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

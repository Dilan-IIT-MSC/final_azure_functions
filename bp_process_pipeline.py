# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(bp_process_pipeline) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
import json
import pyodbc
import os
from azure.storage.blob import BlobServiceClient, ContentSettings
from openai import OpenAI
import requests

bp_process_pipeline = func.Blueprint()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

@bp_process_pipeline.queue_trigger(
    arg_name="msg", 
    queue_name="story-processing-queue",
    connection="AzureQueueStorageConnectionString"
)

def process_story_from_queue(msg: func.QueueMessage) -> None:
    """
    This function is triggered when a message is added to the story-processing-queue.
    It processes the story using OpenAI to create the enhanced audio and images.
    """
    try:
        message_body = msg.get_body().decode('utf-8')
        message_json = json.loads(message_body)
        story_id = message_json.get('story_id')
        
        if not story_id:
            logging.error("No story_id found in queue message")
            return
        
        logging.info(f"Processing story {story_id} from queue")
        
        conn = pyodbc.connect(os.environ['SqlConnectionString'])
        cursor = conn.cursor()

        cursor.execute('SELECT story_url, title FROM story WHERE id = ?', story_id)
        story_data = cursor.fetchone()
        if not story_data:
            logging.error(f"Story {story_id} not found in database")
            return

        story_url = story_data[0]
        story_title = story_data[1]

        blob_service = BlobServiceClient.from_connection_string(os.environ['AzureBlobStorageConnectionString'])
        container_client = blob_service.get_container_client(os.environ['AudioStorageContainerName'])
        
        blob_name = story_url.split('/')[-1]
        blob_path = '/'.join(story_url.split('/')[-3:]) 
        audio_path = f"/tmp/{blob_name}"
        
        blob_client = container_client.get_blob_client(blob_path)
        with open(audio_path, "wb") as f:
            f.write(blob_client.download_blob().readall())

        logging.info(f"Transcribing audio for story {story_id}")
        with open(audio_path, "rb") as f:
            transcription = openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=f
            )
        transcript_text = transcription.text.strip()
        
        if not transcript_text:
            logging.error(f"Failed to transcribe audio for story {story_id}")
            return
        
        # Step 2: Analyze sentiment
        logging.info(f"Analyzing sentiment for story {story_id}")
        sentiment_response = openai_client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": "Classify the emotional tone of a story. Answer only with one word: Positive, Negative, or Neutral."},
                {"role": "user", "content": transcript_text}
            ],
            max_tokens=10
        )
        sentiment = sentiment_response.choices[0].message.content.strip()
        
        # Step 3: Create enhanced script based on sentiment
        logging.info(f"Creating enhanced script for story {story_id} with sentiment: {sentiment}")
        script_response = openai_client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": f"Rewrite the story to strongly highlight {sentiment} emotions while keeping the core narrative intact. Make it vivid, expressive, and easy to narrate aloud. Keep the story length similar to the original."},
                {"role": "user", "content": transcript_text}
            ],
            max_tokens=1500
        )
        enhanced_script = script_response.choices[0].message.content.strip()
        
        # Step 4: Generate audio from enhanced script
        logging.info(f"Generating TTS audio for story {story_id}")
        tts_response = openai_client.audio.speech.create(
            model="tts-1",
            voice="nova",
            input=enhanced_script
        )
        generated_audio = tts_response.content
        
        gen_audio_blob_name = f"generated/{story_id}_narration.mp3"
        gen_audio_container = os.environ.get('GeneratedAudioContainerName', os.environ['AudioStorageContainerName'])
        gen_audio_blob_client = blob_service.get_blob_client(container=gen_audio_container, blob=gen_audio_blob_name)
        gen_audio_blob_client.upload_blob(generated_audio, overwrite=True, content_settings=ContentSettings(content_type="audio/mpeg"))
        gen_audio_url = gen_audio_blob_client.url
        
        cursor.execute('UPDATE story SET gen_audio_url = ? WHERE id = ?', gen_audio_url, story_id)
        
        logging.info(f"Extracting key points for story {story_id}")
        keypoints_response = openai_client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": """Extract 8-10 key moments from the story with appropriate timestamps. 
                The script will be narrated, so distribute the timestamps throughout the duration.
                Format each point as: {timestamp_seconds}|{key_point_description}
                For example: 30|The protagonist faces their biggest fear
                Make each key point visually descriptive and meaningful."""},
                {"role": "user", "content": f"Story title: {story_title}\n\nScript to narrate:\n{enhanced_script}\n\nAssume the narration will take about 3-5 minutes. Distribute timestamps appropriately throughout."}
            ],
            max_tokens=600
        )
        keypoints_text = keypoints_response.choices[0].message.content.strip()

        key_points = []
        for line in keypoints_text.split('\n'):
            if '|' in line:
                parts = line.strip().split('|', 1)
                if len(parts) == 2:
                    try:
                        timestamp = int(parts[0].strip())
                        description = parts[1].strip()
                        key_points.append((timestamp, description))
                    except ValueError:
                        continue
        
        if not key_points:
            logging.warning(f"Failed to parse key points with timestamps, using evenly distributed points")
            points_response = openai_client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": "Extract exactly 8-10 key moments from the story. Each key point must be visually descriptive (max 15 words)."},
                    {"role": "user", "content": enhanced_script}
                ],
                max_tokens=400
            )
            points_list = [p.strip('- 1234567890.').strip() for p in points_response.choices[0].message.content.strip().split('\n') if p.strip()]
            points_list = [p for p in points_list if p][:10]
            
            total_points = len(points_list)
            interval = 180 / (total_points or 1)
            key_points = [(int(i * interval), points_list[i]) for i in range(total_points)]
        
        logging.info(f"Generating {len(key_points)} images for key points")
        images_container_client = blob_service.get_container_client(os.environ.get('StoryImagesContainerName', 'storyimages'))
        
        cursor.execute('DELETE FROM timeline_color WHERE story_id = ?', story_id)
        
        if sentiment.lower() == 'positive':
            colors = ["#91F5AD", "#A8E6CF", "#DCEDC1", "#FFD3B6", "#FFAAA5", "#FF8B94"]
        elif sentiment.lower() == 'negative':
            colors = ["#2C3E50", "#34495E", "#7F8C8D", "#95A5A6", "#BDC3C7", "#ECF0F1"]
        else:  # neutral
            colors = ["#F9F9F9", "#E3E3E3", "#CECECE", "#A8A8A8", "#787878", "#5D5D5D"]
        
        for idx, (timestamp, point) in enumerate(key_points):
            try:
                color = colors[idx % len(colors)]
                image_prompt = f"Digital illustration for a story: '{point}'. Emotional tone: {sentiment}. Style: colorful, emotional, vivid. Suitable for storytelling. Scene must be visually striking and memorable. No text."
                image_response = openai_client.images.generate(
                    model="dall-e-3",
                    prompt=image_prompt,
                    n=1,
                    size="1024x1024"
                )
                image_url = image_response.data[0].url
                
                image_content = requests.get(image_url).content
                image_blob_name = f"{story_id}/{idx+1}.png"
                image_blob_client = images_container_client.get_blob_client(image_blob_name)
                image_blob_client.upload_blob(image_content, overwrite=True, content_settings=ContentSettings(content_type="image/png"))
                
                time_str = f"00:{timestamp//60:02d}:{timestamp%60:02d}"
                
                cursor.execute(
                    'INSERT INTO timeline_color (story_id, time, color) VALUES (?, ?, ?)',
                    story_id, time_str, color
                )
                
                logging.info(f"Created timeline event at {time_str} with color {color}")
            except Exception as img_error:
                logging.error(f"Error processing image {idx+1}: {str(img_error)}")
        
        conn.commit()
        logging.info(f"Successfully processed story {story_id}")
        
    except Exception as e:
        logging.error(f"Exception during story processing from queue: {str(e)}")
        if 'conn' in locals() and conn:
            try:
                conn.rollback()
            except:
                pass
    finally:
        if 'conn' in locals() and conn:
            try:
                conn.close()
            except:
                pass

@bp_process_pipeline.route(route="story/process/test", methods=["POST"])
def test_story_processing(req: func.HttpRequest) -> func.HttpResponse:
    """
    Test function to directly process a story with OpenAI.
    Send a POST request with JSON body: {"story_id": 123}
    """
    try:
        req_body = req.get_json()
        story_id = req_body.get('story_id')
        
        if not story_id:
            return func.HttpResponse(
                body=json.dumps({"status": False, "message": "Missing story_id in request body"}),
                mimetype="application/json",
                status_code=400
            )
        
        logging.info(f"TEST: Starting manual processing for story {story_id}")
        
        conn = pyodbc.connect(os.environ['SqlConnectionString'])
        cursor = conn.cursor()

        cursor.execute('SELECT story_url, title FROM story WHERE id = ?', story_id)
        story_data = cursor.fetchone()
        if not story_data:
            return func.HttpResponse(
                body=json.dumps({"status": False, "message": f"Story {story_id} not found in database"}),
                mimetype="application/json",
                status_code=404
            )

        story_url = story_data[0]
        story_title = story_data[1]
        
        logging.info(f"TEST: Found story: {story_title}, URL: {story_url}")

        blob_service = BlobServiceClient.from_connection_string(os.environ['AzureBlobStorageConnectionString'])
        container_client = blob_service.get_container_client(os.environ['AudioStorageContainerName'])
        
        url_parts = story_url.split('/')
        blob_name = url_parts[-1]
        
        if len(url_parts) >= 3:
            for i in range(len(url_parts) - 2, 0, -1):
                if url_parts[i].isdigit() and url_parts[i+1].isdigit():
                    blob_path = '/'.join(url_parts[i:])
                    break
            else:
                blob_path = blob_name
        else:
            blob_path = blob_name
            
        logging.info(f"TEST: Using blob path: {blob_path}")
        
        audio_path = f"/tmp/{blob_name}"
        
        try:
            blob_client = container_client.get_blob_client(blob_name)
            with open(audio_path, "wb") as f:
                f.write(blob_client.download_blob().readall())
            logging.info(f"TEST: Downloaded audio using simple blob name: {blob_name}")
        except Exception as e:
            logging.warning(f"TEST: Failed to download using simple blob name: {str(e)}")
            try:
                blob_client = container_client.get_blob_client(blob_path)
                with open(audio_path, "wb") as f:
                    f.write(blob_client.download_blob().readall())
                logging.info(f"TEST: Downloaded audio using full blob path: {blob_path}")
            except Exception as e2:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False, 
                        "message": f"Failed to download audio file: {str(e2)}",
                        "blob_name": blob_name,
                        "blob_path": blob_path,
                        "story_url": story_url
                    }),
                    mimetype="application/json",
                    status_code=500
                )

        logging.info(f"TEST: Transcribing audio for story {story_id}")
        try:
            with open(audio_path, "rb") as f:
                transcription = openai_client.audio.transcriptions.create(
                    model="whisper-1",
                    file=f
                )
            transcript_text = transcription.text.strip()
            
            if not transcript_text:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False, 
                        "message": "Failed to transcribe audio: empty result"
                    }),
                    mimetype="application/json",
                    status_code=500
                )
                
            logging.info(f"TEST: Transcription result ({len(transcript_text)} chars): {transcript_text[:100]}...")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error during transcription: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        logging.info(f"TEST: Analyzing sentiment for story {story_id}")
        try:
            sentiment_response = openai_client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": "Classify the emotional tone of a story. Answer only with one word: Positive, Negative, or Neutral."},
                    {"role": "user", "content": transcript_text}
                ],
                max_tokens=10
            )
            sentiment = sentiment_response.choices[0].message.content.strip()
            logging.info(f"TEST: Sentiment analysis result: {sentiment}")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error during sentiment analysis: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        logging.info(f"TEST: Creating enhanced script with sentiment: {sentiment}")
        try:
            script_response = openai_client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": f"Rewrite the story to strongly highlight {sentiment} emotions while keeping the core narrative intact. Make it vivid, expressive, and easy to narrate aloud. Keep the story length similar to the original."},
                    {"role": "user", "content": transcript_text}
                ],
                max_tokens=1500
            )
            enhanced_script = script_response.choices[0].message.content.strip()
            logging.info(f"TEST: Enhanced script ({len(enhanced_script)} chars): {enhanced_script[:100]}...")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error creating enhanced script: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        logging.info(f"TEST: Generating TTS audio")
        try:
            tts_response = openai_client.audio.speech.create(
                model="tts-1",
                voice="nova",
                input=enhanced_script
            )
            generated_audio = tts_response.content
            logging.info(f"TEST: Generated audio size: {len(generated_audio)} bytes")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error generating TTS audio: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        gen_audio_blob_name = f"generated/{story_id}_narration.mp3"
        gen_audio_container = os.environ.get('GeneratedAudioContainerName', os.environ['AudioStorageContainerName'])
        
        try:
            gen_audio_blob_client = blob_service.get_blob_client(container=gen_audio_container, blob=gen_audio_blob_name)
            gen_audio_blob_client.upload_blob(generated_audio, overwrite=True, content_settings=ContentSettings(content_type="audio/mpeg"))
            gen_audio_url = gen_audio_blob_client.url
            logging.info(f"TEST: Uploaded generated audio to: {gen_audio_url}")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error uploading generated audio: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        try:
            cursor.execute('UPDATE story SET gen_audio_url = ? WHERE id = ?', gen_audio_url, story_id)
            logging.info(f"TEST: Updated story table with generated audio URL")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error updating story table: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        logging.info(f"TEST: Extracting key points")
        try:
            keypoints_response = openai_client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[
                    {"role": "system", "content": """Extract 8-10 key moments from the story with appropriate timestamps. 
                    The script will be narrated, so distribute the timestamps throughout the duration.
                    Format each point as: {timestamp_seconds}|{key_point_description}
                    For example: 30|The protagonist faces their biggest fear
                    Make each key point visually descriptive and meaningful."""},
                    {"role": "user", "content": f"Story title: {story_title}\n\nScript to narrate:\n{enhanced_script}\n\nAssume the narration will take about 3-5 minutes. Distribute timestamps appropriately throughout."}
                ],
                max_tokens=600
            )
            keypoints_text = keypoints_response.choices[0].message.content.strip()
            logging.info(f"TEST: Keypoints raw result: {keypoints_text}")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error extracting key points: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        key_points = []
        for line in keypoints_text.split('\n'):
            if '|' in line:
                parts = line.strip().split('|', 1)
                if len(parts) == 2:
                    try:
                        timestamp = int(parts[0].strip())
                        description = parts[1].strip()
                        key_points.append((timestamp, description))
                    except ValueError:
                        continue
        
        if not key_points:
            logging.warning(f"TEST: Failed to parse key points, using evenly distributed points")
            try:
                points_response = openai_client.chat.completions.create(
                    model="gpt-4-turbo",
                    messages=[
                        {"role": "system", "content": "Extract exactly 8-10 key moments from the story. Each key point must be visually descriptive (max 15 words)."},
                        {"role": "user", "content": enhanced_script}
                    ],
                    max_tokens=400
                )
                points_list = [p.strip('- 1234567890.').strip() for p in points_response.choices[0].message.content.strip().split('\n') if p.strip()]
                points_list = [p for p in points_list if p][:10]
                
                total_points = len(points_list)
                interval = 180 / (total_points or 1)
                key_points = [(int(i * interval), points_list[i]) for i in range(total_points)]
                logging.info(f"TEST: Created {len(key_points)} evenly distributed key points")
            except Exception as e:
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False, 
                        "message": f"Error creating fallback key points: {str(e)}"
                    }),
                    mimetype="application/json",
                    status_code=500
                )
        
        logging.info(f"TEST: Generating {len(key_points)} images for key points")
        images_container_name = os.environ.get('StoryImagesContainerName', 'storyimages')
        try:
            images_container_client = blob_service.get_container_client(images_container_name)
            logging.info(f"TEST: Using image container: {images_container_name}")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error accessing image container: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        try:
            cursor.execute('DELETE FROM story_timeline_events WHERE story_id = ?', story_id)
            logging.info(f"TEST: Cleared existing timeline entries")
        except Exception as e:
            return func.HttpResponse(
                body=json.dumps({
                    "status": False, 
                    "message": f"Error clearing timeline entries: {str(e)}"
                }),
                mimetype="application/json",
                status_code=500
            )
        
        if sentiment.lower() == 'positive':
            colors = ["#91F5AD", "#A8E6CF", "#DCEDC1", "#FFD3B6", "#FFAAA5", "#FF8B94"]
        elif sentiment.lower() == 'negative':
            colors = ["#2C3E50", "#34495E", "#7F8C8D", "#95A5A6", "#BDC3C7", "#ECF0F1"]
        else:  # neutral
            colors = ["#F9F9F9", "#E3E3E3", "#CECECE", "#A8A8A8", "#787878", "#5D5D5D"]
        
        # test_limit = min(2, len(key_points))
        # key_points = key_points[:test_limit]
        # logging.info(f"TEST: Limited to {test_limit} keypoints for testing")
        
        timeline_events = []
        success_count = 0
        
        for idx, (timestamp, point) in enumerate(key_points):
            try:
                color = colors[idx % len(colors)]
                logging.info(f"TEST: Processing keypoint {idx+1}: {point} at {timestamp}s with color {color}")

                image_prompt = f"Digital illustration for a story: '{point}'. Emotional tone: {sentiment}. Style: colorful, emotional, vivid. Suitable for storytelling. Scene must be visually striking and memorable. No text."
                
                logging.info(f"TEST: Generating image with prompt: {image_prompt}")
                image_response = openai_client.images.generate(
                    model="dall-e-3",
                    prompt=image_prompt,
                    n=1,
                    size="1024x1024"
                )
                image_url = image_response.data[0].url
                logging.info(f"TEST: Image generated: {image_url}")
                
                image_content = requests.get(image_url).content
                image_blob_name = f"{story_id}/{idx+1}.png"
                image_blob_client = images_container_client.get_blob_client(image_blob_name)
                image_blob_client.upload_blob(image_content, overwrite=True, content_settings=ContentSettings(content_type="image/png"))
                image_blob_url = image_blob_client.url
                logging.info(f"TEST: Uploaded image to blob: {image_blob_name}")
                
                time_str = f"00:{timestamp//60:02d}:{timestamp%60:02d}"
                
                cursor.execute('''
                    INSERT INTO story_timeline_events 
                    (story_id, time, color, image_url) 
                    VALUES (?, ?, ?, ?)
                ''', story_id, time_str, color, image_blob_url)
                
                timeline_events.append({
                    "time": time_str,
                    "color": color,
                    "image_url": image_blob_url
                })
                success_count += 1
                logging.info(f"TEST: Created timeline event at {time_str} with color {color}")
                
            except Exception as img_error:
                logging.error(f"TEST: Error processing image {idx+1}: {str(img_error)}")
                return func.HttpResponse(
                    body=json.dumps({
                        "status": False, 
                        "message": f"Error processing image {idx+1}: {str(img_error)}",
                        "processed_so_far": success_count
                    }),
                    mimetype="application/json",
                    status_code=500
                )
        
        conn.commit()
        logging.info(f"TEST: Successfully processed story {story_id}")
        
        return func.HttpResponse(
            body=json.dumps({
                "status": True,
                "message": "Story processed successfully",
            }, default=str),
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"TEST: Exception during story processing: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({
                "status": False, 
                "message": f"Internal server error: {str(e)}"
            }),
            mimetype="application/json",
            status_code=500
        )
    finally:
        if 'conn' in locals() and conn:
            try:
                conn.close()
            except:
                pass
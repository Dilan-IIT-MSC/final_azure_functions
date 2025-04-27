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
from azure.storage.blob import BlobServiceClient
from openai import OpenAI, Assistant, function_tool, Agent
import asyncio

bp_process_pipeline = func.Blueprint()

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

@function_tool
def transcribe_audio(file_path: str) -> str:
    with open(file_path, "rb") as f:
        transcription = openai_client.audio.transcriptions.create(
            model="whisper-1",
            file=f
        )
    return transcription.text.strip()

@function_tool
def analyze_sentiment(text: str) -> str:
    response = openai_client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": "Classify the emotional tone of a story. Answer only with one word: Positive, Negative, or Neutral."},
            {"role": "user", "content": text}
        ],
        max_tokens=10
    )
    return response.choices[0].message.content.strip()

@function_tool
def rewrite_story(text: str, sentiment: str) -> str:
    response = openai_client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": f"Rewrite the story to strongly highlight {sentiment} emotions. Make it vivid, expressive, and easy to narrate aloud. Keep the story short, under 400 words."},
            {"role": "user", "content": text}
        ],
        max_tokens=800
    )
    return response.choices[0].message.content.strip()

@function_tool
def generate_tts_audio(script: str) -> bytes:
    tts_response = openai_client.audio.speech.create(
        model="tts-1",
        voice="nova",
        input=script
    )
    return tts_response.content

@function_tool
def extract_key_points(script: str) -> list:
    response = openai_client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": "Extract exactly 8-10 key moments from the story. Each key point must be short (max 12 words) and ready to illustrate visually. Answer as a numbered list."},
            {"role": "user", "content": script}
        ],
        max_tokens=400
    )
    points_text = response.choices[0].message.content.strip()
    return [line.strip('- ').strip() for line in points_text.split('\n') if line.strip()]

@function_tool
def generate_image(prompt: str) -> str:
    image_response = openai_client.images.generate(
        model="dall-e-3",
        prompt=prompt,
        n=1,
        size="1024x1024"
    )
    return image_response.data[0].url

@bp_process_pipeline.route(route="story/process", methods=["POST"])
def process_story(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        story_id = req_body.get('story_id')
        if not story_id:
            return func.HttpResponse("Missing story_id", status_code=400)

        conn = pyodbc.connect(os.environ['SqlConnectionString'])
        cursor = conn.cursor()

        cursor.execute('SELECT story_url FROM story WHERE id = ?', story_id)
        story_data = cursor.fetchone()
        if not story_data:
            return func.HttpResponse("Story not found", status_code=404)

        story_url = story_data[0]

        blob_service = BlobServiceClient.from_connection_string(os.environ['AzureBlobStorageConnectionString'])
        container = os.environ['AudioStorageContainerName']
        blob_client = blob_service.get_blob_client(container=container, blob=story_url.split('/')[-1])
        audio_path = f"/tmp/{story_url.split('/')[-1]}"

        with open(audio_path, "wb") as f:
            f.write(blob_client.download_blob().readall())

        agent = Agent(
            name="Story Processing Agent",
            instructions="You are an assistant that processes user-uploaded stories into multimedia formats.",
            model="gpt-4-turbo",
            tools=[
                transcribe_audio,
                analyze_sentiment,
                rewrite_story,
                generate_tts_audio,
                extract_key_points,
                generate_image
            ]
        )

        with Assistant(agent) as assistant:
            result = assistant.run({"file_path": audio_path})

        transcript_text = result.get("transcribe_audio")
        sentiment = result.get("analyze_sentiment")
        creative_script = result.get("rewrite_story")
        generated_audio = result.get("generate_tts_audio")
        key_points = result.get("extract_key_points")

        # Upload generated narration
        gen_audio_blob_name = f"generated/{story_id}_narration.mp3"
        gen_audio_blob_client = blob_service.get_blob_client(container=container, blob=gen_audio_blob_name)
        gen_audio_blob_client.upload_blob(generated_audio, overwrite=True, content_type="audio/mpeg")
        gen_audio_url = gen_audio_blob_client.url

        cursor.execute('UPDATE story SET gen_audio_url = ? WHERE id = ?', gen_audio_url, story_id)

        timeline_events = []
        for idx, point in enumerate(key_points[:10]):
            image_prompt = f"Digital illustration of: '{point}'. Style: colorful, emotional, vivid. Suitable for storytelling. Avoid text or logos."
            image_url = generate_image(image_prompt)

            image_blob_name = f"storyimages/{story_id}_{idx+1}.png"
            image_blob_client = blob_service.get_blob_client(container=os.environ['StoryImagesContainerName'], blob=image_blob_name)
            image_data = openai_client._client.get(image_url).content
            image_blob_client.upload_blob(image_data, overwrite=True, content_type="image/png")

            timeline_events.append((story_id, idx * 10, "#FFFFFF", image_blob_name))

        for event in timeline_events:
            cursor.execute(
                'INSERT INTO timeline_color (story_id, time, color, image) VALUES (?, ?, ?, ?)',
                event[0], event[1], event[2], event[3]
            )

        conn.commit()

        return func.HttpResponse("Story processing completed successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Exception during story processing: {str(e)}")
        return func.HttpResponse(f"Internal server error: {str(e)}", status_code=500)

    finally:
        if 'conn' in locals():
            conn.close()
import logging
from app import app, Company
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
from pytube import YouTube
from app.modules.io_output import io_audio_stream

# /// YouTube ////////////

@login_required
@app.route('/get_youtube_sound', methods=['GET', 'POST'])
def get_youtube_sound():
    """to get sound from YouTube video via link"""
    if request.method == 'POST':
        youtube_url = request.form['text_input']
        try:
            # Create a YouTube object
            yt = YouTube(youtube_url)

            # Get the audio stream with the highest quality
            audio_stream = yt.streams.filter(only_audio=True).first()
            if audio_stream:
                # Download the audio stream to a BytesIO object using the separate function
                audio_buffer = io_audio_stream(audio_stream)

                # Get the name of the YouTube video (you can further sanitize the name if needed)
                video_name = yt.title

                # Send the audio file as a response with the video name as the filename
                return send_file(audio_buffer, download_name=f'{video_name}.mp3', as_attachment=True, mimetype='audio/mpeg')

            return "Audio stream not found."
        except Exception as e:
            return f"Error: {str(e)}"

    return render_template('upload_youtube_sound.html', doc_string=get_youtube_sound.__doc__)

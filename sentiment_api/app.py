from flask import Flask, request, jsonify

app = Flask(__name__)

_get_sentiment_logic = None

try:
    from textblob import TextBlob

    def _textblob_sentiment(text):
        """Sentiment analysis using TextBlob."""
        if not text or not isinstance(text, str) or text.strip() == '':
            return None
        try:
            analysis = TextBlob(text)
            polarity_score = analysis.sentiment.polarity

            if polarity_score > 0.6:
                return 'Positive'
            elif polarity_score < -0.6:
                return 'Negative'
            else:
                return 'Neutral'
            
        except Exception as e:
            print(f"Error calculating sentiment in service (TextBlob) for text: '{text[:50]}...'. Error: {e}")
            return None
    
    _get_sentiment_logic = _textblob_sentiment
    print("TextBlob sentiment logic loaded.")

except ImportError:
    print("TextBlob not found. Falling back to simple keyword-based sentiment in service.")

    def _keyword_sentiment(text):
        """Sentiment analysis using simple keywords."""
        if not text or not isinstance(text, str) or text.strip() == '':
            return None
        text_lower = text.lower()
        positive_keywords = ['great', 'excellent', 'amazing', 'love', 'good', 'happy', 'perfect', 'nice', 'best', 'awesome', 'recommend', '👍']
        negative_keywords = ['bad', 'terrible', 'horrible', 'poor', 'disappointed', 'waste', 'broken', 'not good', 'awful', 'unhappy', '👎']
        
        positive_score = sum(1 for keyword in positive_keywords if keyword in text_lower)
        negative_score = sum(1 for keyword in negative_keywords if keyword in text_lower)

        if positive_score > negative_score:
            return 'Positive'
        elif negative_score > positive_score:
            return 'Negative'
        else:
            return 'Neutral'
            
    _get_sentiment_logic = _keyword_sentiment

if _get_sentiment_logic is None:
    raise RuntimeError("No sentiment logic could be loaded!")


@app.route('/sentiment', methods=['POST'])
def analyze_sentiment():
    data = request.get_json()
    if not data or 'texts' not in data or not isinstance(data['texts'], list):
        return jsonify({"error": "Missing or invalid 'texts' (must be a list) in request body"}), 400

    texts = data['texts']
    results = []
    for text in texts:
        sentiment = _get_sentiment_logic(text)
        results.append(sentiment)

    return jsonify({"sentiments": results})

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "sentiment-api"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
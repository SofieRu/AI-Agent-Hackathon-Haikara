from flask import Flask, request, jsonify
from typing import Callable
import json

class BecknCallbackHandler:
    """
    Handles asynchronous callbacks from Beckn network
    Implements: on_search, on_select, on_init, on_confirm, on_status, on_update
    """
    
    def __init__(self):
        self.callbacks = {
            'on_search': [],
            'on_select': [],
            'on_init': [],
            'on_confirm': [],
            'on_status': [],
            'on_update': []
        }
    
    def register_callback(self, callback_type: str, handler: Callable):
        """Register a callback handler"""
        if callback_type in self.callbacks:
            self.callbacks[callback_type].append(handler)
    
    async def handle_on_search(self, payload: dict):
        """Handle on_search callback"""
        print(f"📨 Received on_search callback")
        
        for handler in self.callbacks['on_search']:
            await handler(payload)
        
        return {"message": {"ack": {"status": "ACK"}}}
    
    async def handle_on_select(self, payload: dict):
        """Handle on_select callback"""
        print(f"📨 Received on_select callback")
        
        for handler in self.callbacks['on_select']:
            await handler(payload)
        
        return {"message": {"ack": {"status": "ACK"}}}
    
    async def handle_on_confirm(self, payload: dict):
        """Handle on_confirm callback"""
        print(f"📨 Received on_confirm callback")
        
        for handler in self.callbacks['on_confirm']:
            await handler(payload)
        
        return {"message": {"ack": {"status": "ACK"}}}


def create_callback_server(handler: BecknCallbackHandler, port: int = 8080):
    """Create Flask server for Beckn callbacks"""
    
    app = Flask(__name__)
    
    @app.route('/beckn/on_search', methods=['POST'])
    async def on_search():
        payload = request.json
        response = await handler.handle_on_search(payload)
        return jsonify(response)
    
    @app.route('/beckn/on_select', methods=['POST'])
    async def on_select():
        payload = request.json
        response = await handler.handle_on_select(payload)
        return jsonify(response)
    
    @app.route('/beckn/on_confirm', methods=['POST'])
    async def on_confirm():
        payload = request.json
        response = await handler.handle_on_confirm(payload)
        return jsonify(response)
    
    return app
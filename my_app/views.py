from django.http import HttpResponse, JsonResponse
import time
import json
import libhoney
from rest_framework import generics, views, response, status, filters, viewsets

from django.shortcuts import render

from honeycomb.socket import send_socket_message
from honeycomb.socket_client import broker


def index(request):
    return HttpResponse("Hello, world!")


libhoney.init(writekey='cQDbjNT0FAoOYWFA5XJwBC', dataset='telemetry')


""" 
def log_event(name, message, view, request_type, status_code=None, email=None, error=None, metadata=None):
    event = libhoney.new_event()
    payload = {
        'status': 'error' if error  else 'success',
        'status_code': status_code,
        'error': error,
        'email': email,
        'name': name,
        'message': message,
        'view': view,
        'request_type': request_type,
        'metadata': json.dumps(metadata) if metadata else None,
    }

    event.add(payload)
    event.send()

    return HttpResponse(payload, request_type, status_code)


# 1. Basic successful login
log_event('login', 'User logged in successfully', 'login_page', 'POST', status_code=200, email='user@example.com')

# 2. Failed login attempt
log_event('login_failure', 'Invalid credentials', 'login_page', 'POST', status_code=401, error='Authentication failed')

# 3. User registration
log_event('register', 'New user registered', 'registration_page', 'POST', status_code=201, email='newuser@example.com')

# 4. Password reset request
log_event('password_reset', 'Password reset requested', 'forgot_password_page', 'POST', email='user@example.com')

# 5. View product page
log_event('view_product', 'User viewed product', 'product_page', 'GET', metadata={'product_id': '12345'})

# 6. Add to cart
log_event('add_to_cart', 'Product added to cart', 'product_page', 'POST', metadata={'product_id': '12345', 'quantity': 2})

# 7. Checkout process
log_event('checkout', 'User initiated checkout', 'checkout_page', 'POST', email='user@example.com', metadata={'total_amount': 99.99})

# 8. Order confirmation
log_event('order_confirmation', 'Order placed successfully', 'order_confirmation_page', 'GET', status_code=200, email='user@example.com', metadata={'order_id': 'ORD-123456'})

# 9. Customer support ticket
log_event('support_ticket', 'New support ticket created', 'support_page', 'POST', email='user@example.com', metadata={'ticket_id': 'TICKET-789'})

# 10. API call error
log_event('api_error', 'External API call failed', 'backend_process', 'GET', status_code=500, error='API timeout', metadata={'api_endpoint': 'https://api.example.com/data'})

log_event('oximetro', 'Medición exitosa', 'Signos vitales', 'POST',  metadata=[{'cabina': '34' }, {'freq cardiacia': 'valor'}, {'oxigenación': 'valor'}])


count = 0
while True:
    count = count + 1
    time.sleep(1)
    print(f'{count} seconds elapsed')
    if count == 10:
        break
"""


class SendHoneyCombData(views.APIView):
    def options(self, request, *args, **kwargs):
        """Manejar solicitudes OPTIONS para CORS preflight."""
        response = HttpResponse()
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response

    def post(self, request):
        req = json.loads(self.request.body)
        data = req
        event = libhoney.new_event()
        payload = data

        # Simular un proceso de espera
        count = 0
        while True:
            count += 1
            time.sleep(1)
            print(f'{count} seconds elapsed')
            if count == 10:
                break

        event.add(payload)
        event.send()

        # Responder con los encabezados CORS
        response = JsonResponse({'result': True, 'data': data})
        response['Access-Control-Allow-Origin'] = '*'  # Cambia '*' por tu dominio de origen si es necesario
        return response

# Height
# class SendHeightSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "height"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendHeightSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "height"
        }

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message")
            if not isinstance(inner, dict):
                return False

            return inner.get("vs") == "height"

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": result["message"],     
            },
            status=200,
        )

# Weight
# class SendWeightSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "weight"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendWeightSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "weight"
        }

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message")
            if not isinstance(inner, dict):
                return False

            return inner.get("vs") == "weight"

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": result["message"],     
            },
            status=200,
        )


# Pressure
# class SendPressureSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "esfigmo"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendPressureSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "esfigmo"
        }

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message")
            if not isinstance(inner, dict):
                return False

            return inner.get("vs") == "esfigmo"

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": result["message"],     
            },
            status=200,
        )


# Oxygen
# class SendOxygenSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "oxygen"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendOxygenSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "oxygen",
        }

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message")
            if not isinstance(inner, dict):
                return False

            return inner.get("vs") == "oxygen"

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": result["message"],     
            },
            status=200,
        )



# Temperature
# class SendTemperatureSocketMessage(views.APIView):
#     def post(self, request):
#         req = json.loads(self.request.body)
#         cabin = req['channel']
#         message = {
#             "type": "command",
#             "vital-sign": "temperature"
#         }
#         send_socket_message(f"{cabin}-cmd", message)
#         return HttpResponse(json.dumps({'result': True}), content_type='application/json')

class SendTemperatureSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req["channel"]

        sub_channel = cabin
        pub_channel = f"{cabin}-cmd"

        message = {
            "type": "command",
            "vital-sign": "temperature"
        }

        def predicate(msg: dict) -> bool:
            if not isinstance(msg, dict):
                return False

            inner = msg.get("message")
            if not isinstance(inner, dict):
                return False

            return inner.get("vs") == "temperature"

        result = broker.publish_and_wait(
            sub_channel=sub_channel,
            pub_channel=pub_channel,
            message=message,
            predicate=predicate,
            timeout=90,
        )

        if result is None:
            return JsonResponse(
                {
                    "result": False,
                    "error": "Timeout esperando respuesta de la cabina",
                    "channel": sub_channel,
                },
                status=504,
            )

        return JsonResponse(
            {
                "result": True,
                "channel": result["channel"],
                "data": result["message"],     
            },
            status=200,
        )


# Esteto
class SendEstetoSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "command",
            "vital-sign": "mic"
        }
        send_socket_message(f"{cabin}-cmd", message)
        return HttpResponse(json.dumps({'result': True}), content_type='application/json')


# Booth control
class SendPatientExitSocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "navigation",
            "vital-sign": "end-screen"
        }
        send_socket_message(f"{cabin}-cmd", message)
        send_socket_message(cabin, message)

        return HttpResponse(json.dumps({'result': True}), content_type='application/json')


# Emergency
class SendActivateEmergencySocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "command",
            "vital-sign": "e-stop",
        }
        send_socket_message(f"{cabin}-cmd", message)

        return HttpResponse(json.dumps({'result': True}), content_type='application/json')


class SendDeactivateEmergencySocketMessage(views.APIView):
    def post(self, request):
        req = json.loads(self.request.body)
        cabin = req['channel']
        message = {
            "type": "command",
            "vital-sign": "N-e-stop",
        }
        send_socket_message
        return HttpResponse(json.dumps({'result': True}), content_type='application/json')
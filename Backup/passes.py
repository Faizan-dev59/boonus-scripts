from app.crud import wallet_pass
from app.crud import device
from app.utils.helpers import send_notification
from app.utils.generate import generate
from fastapi import APIRouter
from typing import Dict, Any
from datetime import datetime
from app.core.config import settings
import requests
import boto3
from starlette.responses import RedirectResponse
from collections import namedtuple
from app.logs import log_conf

logger = log_conf.Logger(__name__)

router = APIRouter()


@router.post("/generate", status_code=201)
async def create_pass(passbook: Dict[Any, Any] = None):
    """
    triger the genrataion of new pass store it in s3 and return the link
    """

    WalletRequest = namedtuple("WalletRequest", passbook.keys())
    wallet_request = WalletRequest(**passbook)
    name = generate(wallet_request)
    walletId = wallet_request.qrCodeData.split(";")[1]
    s3 = boto3.client(
        "s3",
        region_name=settings.REGION_NAME,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )
    key = f"{wallet_request.businessId}/{walletId}/{name}"
    s3.upload_file(
        Filename=name,
        Bucket=settings.BUCKET_PASS,
        Key=key,
        ExtraArgs={"ACL": "public-read", "ContentType": "application/vnd.apple.pkpass"},
    )
    data = settings.BUCKET_PASS_URL + key

    payload = {
        "passTypeIdentifier": settings.PASS_TYPE_IDENTIFIER,
        "serialNumber": wallet_request.serialNumber,
        "updatedAt": datetime.now(),
        "data": data,
    }
    pass_id = await wallet_pass.post_pass(payload)
    response_object = {
        "id": pass_id,
        "serial_number": payload["serialNumber"],
        "data": data,
    }

    return response_object


@router.put("/update")
async def update(passbook: Dict[Any, Any] = None):
    """
    updating a pass.pkpass by regenrating it from updated json and update the file in s3 bucket
    """
    WalletRequest = namedtuple("WalletRequest", passbook.keys())
    wallet_request = WalletRequest(**passbook)
    payload = {
        "passTypeIdentifier": settings.PASS_TYPE_IDENTIFIER,
        "serialNumber": wallet_request.serialNumber,
    }
    p = await wallet_pass.get_pass(payload)
    if not p:
        return ("Not Authorized", 401)

    name = generate(wallet_request)
    walletId = wallet_request.qrCodeData.split(";")[1]
    s3 = boto3.client(
        "s3",
        region_name=settings.REGION_NAME,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )
    key = f"{wallet_request.businessId}/{walletId}/{name}"
    s3.upload_file(
        Filename=name,
        Bucket=settings.BUCKET_PASS,
        Key=key,
        ExtraArgs={"ACL": "public-read", "ContentType": "application/vnd.apple.pkpass"},
    )

    registrations = await device.get_pass_registrations(p["id"])
    logger.info(" registrations ")
    logger.info(registrations)
    pushTokensAd = []
    for r in registrations:
        if r["device"].split("/")[0] == "WalletPasses":
            pushTokensAd.append(r["push_token"])
        else:
            logger.info("apn role +++++++++++++++++++")
            apn_res = await send_notification(r["push_token"])
            logger.info("apn response: ", apn_res.description)

    if len(pushTokensAd) > 0:
        url = settings.WALLETPASSES_URL
        headers = {"content-type": "application/json"}
        req = {
            "passTypeIdentifier": settings.PASS_TYPE_IDENTIFIER,
            "pushTokens": pushTokensAd,
        }

        logger.info(req)
        for i in range(10):
            req_res = requests.post(url, headers=headers, json=req)
            logger.info("status: ", req_res.status_code)
            logger.info("status: ", req_res.text)
            if req_res.status_code != 429:
                break
    return ("ok", 200)


@router.get("/{pass_type_identifier}/{serial_number}", status_code=301)
async def download_pass(pass_type_identifier, serial_number):
    """
    Pass delivery
    Getting the latest version of a Pass
    GET /v1/passes/<pass_type_identifier>/<serial_number>
    header: Authorization: ApplePass <authenticationToken>
    server response:
    --> if auth token is correct: 301 redirect to pass data in s3
    --> if auth token is incorrect: 401
    """
    payload = {
        "passTypeIdentifier": settings.PASS_TYPE_IDENTIFIER,
        "serialNumber": serial_number,
        "updatedAt": datetime.now(),
        "data": "data",
    }

    item = await wallet_pass.get_pass(payload)
    logger.info(type(item))
    try:
        return RedirectResponse(item["data"])
    except Exception as ex:
        logger.errror(ex)
        return "failed", 500

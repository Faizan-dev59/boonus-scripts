from uuid import uuid4
from aioapns import APNs, NotificationRequest, PushType
from app.core.config import settings
from wallet.models import Pass, Barcode, StoreCard, Alignment, BarcodeFormat
from app.utils.drawing import (
    draw_stamp_icon,
    geneate_full_strip_image,
    draw_rewards,
    create_strip_image_for_points_passs,
)
from collections import namedtuple
from app.core import constants
import boto3
from pathlib import Path

directory = constants.ICONS_DIRECTORY
POINTS = constants.POINTS
STAMPS = constants.STAMPS


async def send_notification(device_token):
    apns_key_client = APNs(
        key=constants.AUTH_KEY_PATH,
        key_id=settings.KEY_ID,
        team_id=settings.TEAM_IDENTIFIER,
        topic=settings.PASS_TYPE_IDENTIFIER,
        use_sandbox=False,
    )
    request = NotificationRequest(
        device_token=device_token,
        message={},
        notification_id=str(uuid4()),
        time_to_live=3,
        push_type=PushType.ALERT,
    )
    return await apns_key_client.send_notification(request)


def delete_change_message(card_info):
    for header_field in card_info.headerFields:
        delattr(header_field, "changeMessage")
    for secondary_field in card_info.secondaryFields:
        delattr(secondary_field, "changeMessage")
    for back_field in card_info.backFields:
        if back_field.key == "last_update":
            continue
        else:
            delattr(back_field, "changeMessage")
    return card_info


def create_wallet_card(wallet_request):
    card_info = StoreCard()
    if wallet_request.nature == STAMPS:
        stamps_ratio = f"{wallet_request.nOfStamps}/{wallet_request.stamps}"
        card_info.addHeaderField("stamps_ratio", stamps_ratio, "الأختام")
        visit_to_next_reward = wallet_request.stamps - wallet_request.nOfStamps
        card_info.addSecondaryField(
            "visit_to_next_reward", visit_to_next_reward, "أختام للهدية القادمة"
        )
    else:
        card_info.addHeaderField("points", wallet_request.points, "النقاط")
        if hasattr(wallet_request, "tier") and wallet_request.tier:
            card_info.addSecondaryField("tier", wallet_request.tier, "المستوى")

    card_info.addSecondaryField("rewards", wallet_request.redeemableRewards, "عدد هداياك")
    textAlignmentIndex = 1 if hasattr(wallet_request, "tier") > 1 else 0
    card_info.secondaryFields[textAlignmentIndex].textAlignment = Alignment.RIGHT

    card_info.addBackField("last_update", wallet_request.lastUpdate, "جديدك")
    card_info.backFields[0].changeMessage = " %@"
    card_info.addBackField("full_name", wallet_request.fullName, "هلا بك")
    if wallet_request.default: # show feedback field if wallet in default pass
        card_info.addBackField(
            "feedback_link",
            f"<a href={wallet_request.feedbackLink}>اضغط هنا للتقييم</a>"
            if wallet_request.feedbackLink
            else " ننتظر زيارتك لتقيمنا بعدها",
            "قيمنا",
        )
    if hasattr(wallet_request, "card_description") and wallet_request.card_description: 
        card_info.addBackField(
            "card_description", wallet_request.cardDescription, "عن بطاقتنا"
        )
    
    if hasattr(wallet_request, "howToUse") and wallet_request.howToUse: 
        card_info.addBackField("howToUse", wallet_request.howToUse, "كيف تستخدم بطاقتنا")

    if wallet_request.nature == POINTS:
        if hasattr(wallet_request, "tierPerks") and wallet_request.tierPerks:
            card_info.addBackField("tierPerks", wallet_request.tierPerks, "مزايا المستوى")
        if isinstance(wallet_request.rewardsReadyToClaim, str) or len(wallet_request.rewardsReadyToClaim) < 1:
            card_info.addBackField(
                "rewards_ready_to_claim",
                wallet_request.rewardsReadyToClaim,
                "هدايا تنتظرك",
            )
        else:
            rewardsReadyToClaim = ""
            RewardsReadyToClaim = namedtuple(
                "RewardsReadyToClaim", wallet_request.rewardsReadyToClaim[0].keys()
            )
            for r in wallet_request.rewardsReadyToClaim:
                reward_ready_to_claim = RewardsReadyToClaim(**r)
                reward_name = reward_ready_to_claim.arabicName
                points = reward_ready_to_claim.points
                if reward_ready_to_claim.claimable is True:
                    link = settings.CLAIM_URL + reward_ready_to_claim.hashCode
                    rewardsReadyToClaim += (
                        f"<a href={link}>{reward_name} - {points} pts</a> \n"
                    )
                else:
                    rewardsReadyToClaim += f"{reward_name} - {points} pts \n"

            card_info.addBackField(
                "rewards_ready_to_claim", rewardsReadyToClaim, "هدايا تنتظرك"
            )

        if isinstance(wallet_request.rewardsReadyToRedeem, str) or len(wallet_request.rewardsReadyToRedeem) < 1:
            card_info.addBackField(
                "rewards_ready_to_redeem",
                wallet_request.rewardsReadyToRedeem,
                "هداياك معانا (اعطي الكود للكاشير)",
            )
        else:
            rewardsReadyToRedeem = ""
            RewardsReadyToRedeem = namedtuple(
                "RewardsReadyToRedeem", wallet_request.rewardsReadyToRedeem[0].keys()
            )
            for b in wallet_request.rewardsReadyToRedeem:
                reward_ready_to_redeem = RewardsReadyToRedeem(**b)
                ready_reward_name = reward_ready_to_redeem.arabicName
                code = reward_ready_to_redeem.code
                rewardsReadyToRedeem += f"{ready_reward_name} : {code} \n"

            card_info.addBackField(
                "rewards_ready_to_redeem",
                rewardsReadyToRedeem,
                "هداياك معانا (اعطي الكود للكاشير)",
            )
    else:
        if isinstance(wallet_request.rewardsReadyToRedeem, str) or len(wallet_request.rewardsReadyToRedeem) < 1:
            card_info.addBackField(
                "rewards_ready_to_redeem",
                wallet_request.rewardsReadyToRedeem,
                "هداياك معانا (اعطي الكود للكاشير)",
            )
        else:
            rewardsReadyToRedeem = ""
            RewardsReadyToRedeem = namedtuple(
                "RewardsReadyToRedeem", wallet_request.rewardsReadyToRedeem[0].keys()
            )
            for b in wallet_request.rewardsReadyToRedeem:
                reward_ready_to_redeem = RewardsReadyToRedeem(**b)
                ready_reward_name = reward_ready_to_redeem.arabicName
                code = reward_ready_to_redeem.code
                rewardsReadyToRedeem += f"{ready_reward_name} : {code} \n"

            card_info.addBackField(
                "rewards_ready_to_redeem",
                rewardsReadyToRedeem,
                "هداياك معانا (اعطي الكود للكاشير)",
            )
  
    if "campaignRewards" in dir(wallet_request):
        if len(wallet_request.campaignRewards):
            rewards2text = "".join(
                [
                    f"{reward['arabicName']} : {reward['code']} \n"
                    for reward in wallet_request.campaignRewards
                ]
            )
            card_info.addBackField(
                "campaign_rewards", rewards2text, "هدايا خاصة لك انت"
            )

    card_info.addBackField("earned_rewards", wallet_request.allRewards, "كل هداياك")
    if hasattr(wallet_request, "menu") and wallet_request.menu: 
        card_info.addBackField(
            "menu", f'<a href={wallet_request.menu}>اضغط هنا للعرض</a>', "قائمة الطلبات"
        )
    if len(wallet_request.branches) > 0:
        Branch = namedtuple("Branch", wallet_request.branches[0].keys())
        branches = ""
        for b in wallet_request.branches:
            branch = Branch(**b)
            name = branch.name
            link = branch.link
            if branch.link:
                branches += f"{name} :\n<a href={link}>الموقع على خرائط جوجل</a> \n"
        card_info.addBackField("branches", branches, "أماكن تواجدنا")

    SocialLink = namedtuple("SocialLink", wallet_request.socialLinks[0].keys())
    socialLinks = ""
    for s in wallet_request.socialLinks:
        social_links = SocialLink(**s)
        name = social_links.name
        link = social_links.link
        if social_links.link:
            socialLinks += f"<a href={link}>{name.upper()}</a> \n"
    if len(socialLinks) > 0:
        card_info.addBackField("social_links", socialLinks, "تابعنا")
    
    BusinessInformation = namedtuple(
        "BusinessInformation", wallet_request.businessInformation.keys()
    )
    business_information = BusinessInformation(**wallet_request.businessInformation)
    if business_information.email or business_information.phone:
        card_info.addBackField(
            "business_info",
            "\n".join([ item for item in (business_information.email, business_information.phone) if item]),
            "اتصل بنا",
        )
    
    if "terms" in dir(wallet_request) and wallet_request.terms:
        card_info.addBackField("terms", wallet_request.terms, "الشروط والأحكام")
    if (wallet_request.businessId == "619bd7a663fb467995beb75b"):
        card_info.addBackField(
            "powered_by", '<a href="https://www.cove.sa/">مقهى كوف للقهوة المختصة</a>', "مزود الخدمة"
        )
    else:
        card_info.addBackField(
            "powered_by", '<a href="https://resal.me/boonus">Resal Boonus</a>', "مزود الخدمة"
        )

    # change textAlignment to right for all fields
    for idx in range(len(card_info.headerFields)):
        card_info.headerFields[idx].textAlignment = Alignment.RIGHT
    for idx in range(len(card_info.secondaryFields)):
        card_info.secondaryFields[idx].textAlignment = Alignment.RIGHT
    for idx in range(len(card_info.backFields)):
        card_info.backFields[idx].textAlignment = Alignment.RIGHT

    delete_change_message(card_info)
    return card_info


def create_passfile(card_info, wallet_request):
    passTypeIdentifier = settings.PASS_TYPE_IDENTIFIER
    teamIdentifier = settings.TEAM_IDENTIFIER
    organizationName = wallet_request.businessName
    passfile = Pass(
        card_info,
        passTypeIdentifier=passTypeIdentifier,
        organizationName=organizationName,
        teamIdentifier=teamIdentifier,
    )

    passfile.serialNumber = wallet_request.serialNumber
    passfile.logoText = wallet_request.logoText
    passfile.barcode = Barcode(
        message=wallet_request.qrCodeData, format=BarcodeFormat.QR
    )
    passfile.webServiceURL = settings.WEB_SERVICE_URL
    passfile.authenticationToken = settings.AUTHENTICATION_TOKEN
    Colors = namedtuple("Colors", wallet_request.colors.keys())
    colors = Colors(**wallet_request.colors)

    passfile.backgroundColor = colors.cardBackground
    passfile.foregroundColor = colors.cardText
    passfile.labelColor = colors.cardText
    if hasattr(wallet_request, "expired") and wallet_request.expired: 
        passfile.voided = True
    s3 = boto3.client(
        "s3",
        region_name=settings.REGION_NAME,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )

    iconName = Path(wallet_request.icon).name
    if (
        "boonus-pass" in wallet_request.icon.split("/")[2]
    ):  # here means that the icon is custom icon and it's uploaded to the pass folder
        walletId = wallet_request.qrCodeData.split(";")[1]
        s3.download_file(
            settings.BUCKET_PASS,
            f"{wallet_request.businessId}/{walletId}/{iconName}",
            f"{directory}icon.png",
        )
    else:
        s3.download_file(settings.BUCKET_GENRAL, iconName, f"{directory}icon.png")

    logoName = Path(wallet_request.logo).name
    if (
        "boonus-pass" in wallet_request.logo.split("/")[2]
    ):  # here means that the logo is custom logo and it's uploaded to the pass folder
        walletId = wallet_request.qrCodeData.split(";")[1]
        s3.download_file(
            settings.BUCKET_PASS,
            f"{wallet_request.businessId}/{walletId}/{logoName}",
            f"{directory}logo.png",
        )
    else:
        s3.download_file(settings.BUCKET_GENRAL, logoName, f"{directory}logo.png")

    stripImagePath = ""
    if colors.stripImage:
        stripImageName = Path(colors.stripImage).name
        stripImagePath = f"{directory}{stripImageName}"
        walletId = wallet_request.qrCodeData.split(";")[1]
        s3.download_file(
            settings.BUCKET_PASS,
            f"{wallet_request.businessId}/{walletId}/{stripImageName}",
            stripImagePath,
        )

    if wallet_request.nature == STAMPS:
        StampedImage = namedtuple("StampedImage", wallet_request.stampedImage.keys())
        stamped_image = StampedImage(**wallet_request.stampedImage)

        stampedImagePath = ""
        if stamped_image.isIcon is False and isinstance(stamped_image.address, str):
            stampedImageName = Path(stamped_image.address).name
            stampedImagePath = f"{directory}{stampedImageName}"
            walletId = wallet_request.qrCodeData.split(";")[1]
            s3.download_file(
                settings.BUCKET_PASS,
                f"{wallet_request.businessId}/{walletId}/{stampedImageName}",
                stampedImagePath,
            )

        UnstampedImage = namedtuple(
            "UnstampedImage", wallet_request.unstampedImage.keys()
        )
        unstamped_image = UnstampedImage(**wallet_request.unstampedImage)

        unstampedImagePath = ""
        if unstamped_image.isIcon is False and isinstance(unstamped_image.address, str):
            unstampedImageName = Path(unstamped_image.address).name
            unstampedImagePath = f"{directory}{unstampedImageName}"
            walletId = wallet_request.qrCodeData.split(";")[1]
            s3.download_file(
                settings.BUCKET_PASS,
                f"{wallet_request.businessId}/{walletId}/{unstampedImageName}",
                unstampedImagePath,
            )

        if stamped_image.isIcon is True:
            draw_stamp_icon(
                f"{directory}icons/{stamped_image.icon}.png",
                "circleS.png",
                colors.stampCircle,
                colors.stampCircleBorder,
                colors.stampedImage,
            )
        elif stamped_image.address:
            draw_stamp_icon(
                stampedImagePath,
                "circleS.png",
                colors.stampCircle,
                colors.stampCircleBorder,
                colors.stampedImage,
            )
        else:
            draw_stamp_icon(
                "" + directory + "check-light-t.png",
                "circleS.png",
                colors.stampCircle,
                colors.stampCircleBorder,
                colors.stampedImage,
            )  # , colors.unstampedImage)

        if unstamped_image.isIcon is True:
            draw_stamp_icon(
                f"{directory}icons/{unstamped_image.icon}.png",
                "circleU.png",
                colors.stampCircle,
                colors.stampCircleBorder,
                colors.unstampedImage,
            )
        elif unstamped_image.address:
            draw_stamp_icon(
                unstampedImagePath,
                "circleU.png",
                colors.stampCircle,
                colors.stampCircleBorder,
                colors.unstampedImage,
            )
        else:
            draw_stamp_icon(
                "" + directory + "check-light-t.png",
                "circleU.png",
                colors.stampCircle,
                colors.stampCircleBorder,
                colors.unstampedImage,
            )

        number_of_remaining_stamps = wallet_request.stamps - wallet_request.nOfStamps
        if wallet_request.type == "single":
            geneate_full_strip_image(
                wallet_request.stamps,
                number_of_remaining_stamps,
                colors.stripColor,
                stripImagePath=stripImagePath,
            )
        else:
            index = draw_rewards(wallet_request.stamps, wallet_request.giftArray)
            geneate_full_strip_image(
                wallet_request.stamps,
                number_of_remaining_stamps,
                colors.stripColor,
                index=index,
                stripImagePath=stripImagePath,
            )
    else:
        if colors.stripImage:
            create_strip_image_for_points_passs(image=stripImagePath)
        else:
            create_strip_image_for_points_passs(color=colors.stripColor)

    passfile.addFile("icon.png", open(f"{directory}icon.png", "rb"))
    passfile.addFile("icon@2x.png", open(f"{directory}icon.png", "rb"))
    passfile.addFile("icon@3x.png", open(f"{directory}icon.png", "rb"))

    passfile.addFile("logo.png", open(f"{directory}logo.png", "rb"))
    passfile.addFile("logo@2x.png", open(f"{directory}logo.png", "rb"))
    passfile.addFile("logo@3x.png", open(f"{directory}logo.png", "rb"))

    passfile.addFile("strip.png", open("strip.png", "rb"))
    passfile.addFile("strip@2x.png", open("strip.png", "rb"))
    passfile.addFile("strip@3x.png", open("strip.png", "rb"))
    return passfile


def create_pkpassfile(passfile, serialNumber):
    password = settings.PASSWORD
    name = f"pass_{serialNumber}.pkpass"
    pkpass = passfile.create(
        "app/tmp/signerCert.pem",
        "app/tmp/signerKey.pem",
        "app/tmp/wwdr.pem",
        password,
        name,
    )
    return pkpass

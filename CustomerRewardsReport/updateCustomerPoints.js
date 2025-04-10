const { MongoClient, ObjectId } = require('mongodb');
const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const { default: axios } = require('axios');
require('dotenv').config();

// MongoDB setup
const uri = process.env.DATABASE_URI;
const client = new MongoClient(uri);
const businessId = new ObjectId('67bc68b4c5ef56001d39f3f3');
const walletId = new ObjectId('67bc6bf9c5ef56001d3ae415');
const csvFilePath = 'demoSheetForPoints.csv';

// Constants (copied from your first file)
const POINTS = 'points';
const CAMPAIGN = 'campaign';
const STAMPS = 'stamps';
const MEMBERSHIP = 'membership';

// Helper function to add backfields to passes
const addPassBackfields = (
  backfields,
  customer,
  feedbackLink,
  redeemedRewardsCount,
  terms,
  rewardsReadyToClaim,
  rewards,
  campaignRewards,
  lastUpdate,
  voucherSource,
  language,
  branches,
  socials,
  catalogue,
  support,
  tierPerks,
  description,
  business_name,
  isDefault,
  website
) => {
  // Customer Information
  backfields.push({
    key: 'Customer',
    value: `${customer.firstName} ${customer.lastName}`,
    label: language === 'ar' ? 'العميل' : 'Customer',
    row: 0,
    section: language === 'ar' ? 'معلومات العميل' : 'Customer Information',
  });

  // Push customer mobile information
  if (customer.phone?.number) {
    backfields.push({
      key: 'Mobile',
      value: `+${customer.phone.countryCode}${customer.phone.number}`,
      label: language === 'ar' ? 'الجوال' : 'Mobile',
      row: 1,
      section: language === 'ar' ? 'معلومات العميل' : 'Customer Information',
    });
  }

  // Push customer email if available
  if (customer.email) {
    backfields.push({
      key: 'Email',
      value: customer.email,
      label: language === 'ar' ? 'البريد الإلكتروني' : 'Email',
      row: 2,
      section: language === 'ar' ? 'معلومات العميل' : 'Customer Information',
    });
  }

  // Feedback link for transactions
  if (feedbackLink) {
    backfields.push({
      key: 'Feedback',
      value: feedbackLink,
      label: language === 'ar' ? 'رابط تقييم الخدمة' : 'Transaction Feedback',
      row: 3,
      section: language === 'ar' ? 'معلومات العميل' : 'Customer Information',
    });
  }

  // Add Program Description if available
  if (description) {
    backfields.push({
      key: 'Description',
      value: description,
      label: language === 'ar' ? 'وصف البرنامج' : 'Program Description',
      row: 0,
      section: language === 'ar' ? 'نظرة عامة' : 'Overview',
    });
  }

  // Add Last update message if available
  if (lastUpdate) {
    backfields.push({
      key: 'LastUpdate',
      value: lastUpdate,
      label: language === 'ar' ? 'آخر تحديث' : 'Last Update',
      row: 1,
      section: language === 'ar' ? 'نظرة عامة' : 'Overview',
    });
  }

  // Add redeemed rewards count
  backfields.push({
    key: 'redeemedRewards',
    value: redeemedRewardsCount || 0,
    label: language === 'ar' ? 'المكافآت المستبدلة' : 'Redeemed Rewards',
    row: 2,
    section: language === 'ar' ? 'نظرة عامة' : 'Overview',
  });

  // Add active rewards
  if (rewards && rewards.length > 0) {
    backfields.push({
      key: 'ActiveRewards',
      value: language === 'ar' ? 'المكافآت النشطة' : 'Active Rewards',
      label: language === 'ar' ? 'المكافآت النشطة' : 'Active Rewards',
      row: 0,
      section: language === 'ar' ? 'المكافآت' : 'Rewards',
    });

    rewards.forEach((reward, index) => {
      backfields.push({
        key: `reward_${index}`,
        value: language === 'ar' ? reward.arabicName : reward.name,
        label: reward.code,
        row: index + 1,
        section: language === 'ar' ? 'المكافآت' : 'Rewards',
      });
    });
  }

  // Add campaign rewards
  if (campaignRewards && campaignRewards.length > 0) {
    if (rewards && rewards.length === 0) {
      backfields.push({
        key: 'CampaignRewards',
        value: language === 'ar' ? 'مكافآت الحملات' : 'Campaign Rewards',
        label: language === 'ar' ? 'مكافآت الحملات' : 'Campaign Rewards',
        row: 0,
        section: language === 'ar' ? 'المكافآت' : 'Rewards',
      });
    }

    let startRow = rewards && rewards.length > 0 ? rewards.length + 1 : 1;
    campaignRewards.forEach((reward, index) => {
      backfields.push({
        key: `campaign_reward_${index}`,
        value: language === 'ar' ? reward.arabicName : reward.name,
        label: reward.code,
        row: startRow + index,
        section: language === 'ar' ? 'المكافآت' : 'Rewards',
      });
    });
  }

  // Add rewards ready to claim
  if (rewardsReadyToClaim && rewardsReadyToClaim.length > 0) {
    backfields.push({
      key: 'RewardsReadyToClaim',
      value:
        language === 'ar'
          ? 'مكافآت جاهزة للإسترداد'
          : 'Rewards Ready to Claim',
      label:
        language === 'ar'
          ? 'مكافآت جاهزة للإسترداد'
          : 'Rewards Ready to Claim',
      row: 0,
      section:
        language === 'ar' ? 'مكافآت جاهزة للإسترداد' : 'Rewards Ready to Claim',
    });

    rewardsReadyToClaim.forEach((reward, index) => {
      backfields.push({
        key: `reward_ready_${index}`,
        value: language === 'ar' ? reward.arabicName : reward.name,
        label: language === 'ar' ? 'إضغط للإسترداد' : 'Click to Claim',
        row: index + 1,
        section:
          language === 'ar'
            ? 'مكافآت جاهزة للإسترداد'
            : 'Rewards Ready to Claim',
      });
    });
  }

  // Add tier perks if available
  if (tierPerks) {
    backfields.push({
      key: 'TierPerks',
      value: language === 'ar' ? 'مزايا المستوى' : 'Tier Perks',
      label: language === 'ar' ? 'مزايا المستوى' : 'Tier Perks',
      row: 0,
      section: language === 'ar' ? 'مزايا المستوى' : 'Tier Perks',
    });

    tierPerks.forEach((perk, index) => {
      backfields.push({
        key: `tier_perk_${index}`,
        value: perk,
        label: `${index + 1}`,
        row: index + 1,
        section: language === 'ar' ? 'مزايا المستوى' : 'Tier Perks',
      });
    });
  }

  // Add branches information
  if (branches && branches.length > 0) {
    backfields.push({
      key: 'Branches',
      value: language === 'ar' ? 'الفروع' : 'Branches',
      label: language === 'ar' ? 'الفروع' : 'Branches',
      row: 0,
      section: language === 'ar' ? 'الفروع' : 'Branches',
    });

    branches.forEach((branch, index) => {
      backfields.push({
        key: `branch_${index}`,
        value: branch.name,
        label: branch.address || `${index + 1}`,
        row: index + 1,
        section: language === 'ar' ? 'الفروع' : 'Branches',
      });
    });
  }

  // Add Terms and Conditions
  if (terms) {
    backfields.push({
      key: 'Terms',
      value: terms,
      label: language === 'ar' ? 'الشروط والأحكام' : 'Terms & Conditions',
      row: 0,
      section: language === 'ar' ? 'الشروط والأحكام' : 'Terms & Conditions',
    });
  }

  // Add Contact Information
  let contactRow = 0;
  if (isDefault) {
    backfields.push({
      key: 'BusinessName',
      value: business_name,
      label: language === 'ar' ? 'اسم المتجر' : 'Business Name',
      row: contactRow++,
      section: language === 'ar' ? 'معلومات التواصل' : 'Contact Information',
    });
  }

  if (website) {
    backfields.push({
      key: 'Website',
      value: website,
      label: language === 'ar' ? 'الموقع الإلكتروني' : 'Website',
      row: contactRow++,
      section: language === 'ar' ? 'معلومات التواصل' : 'Contact Information',
    });
  }

  if (support?.email) {
    backfields.push({
      key: 'SupportEmail',
      value: support.email,
      label: language === 'ar' ? 'البريد الإلكتروني للدعم' : 'Support Email',
      row: contactRow++,
      section: language === 'ar' ? 'معلومات التواصل' : 'Contact Information',
    });
  }

  if (support?.phone) {
    backfields.push({
      key: 'SupportPhone',
      value: support.phone,
      label: language === 'ar' ? 'هاتف الدعم' : 'Support Phone',
      row: contactRow++,
      section: language === 'ar' ? 'معلومات التواصل' : 'Contact Information',
    });
  }

  // Add Social Media Information
  if (socials && Object.keys(socials).length > 0) {
    let socialRow = 0;
    if (socials.instagram) {
      backfields.push({
        key: 'Instagram',
        value: socials.instagram,
        label: 'Instagram',
        row: socialRow++,
        section: language === 'ar' ? 'وسائل التواصل الإجتماعي' : 'Social Media',
      });
    }

    if (socials.twitter) {
      backfields.push({
        key: 'Twitter',
        value: socials.twitter,
        label: 'Twitter',
        row: socialRow++,
        section: language === 'ar' ? 'وسائل التواصل الإجتماعي' : 'Social Media',
      });
    }

    if (socials.facebook) {
      backfields.push({
        key: 'Facebook',
        value: socials.facebook,
        label: 'Facebook',
        row: socialRow++,
        section: language === 'ar' ? 'وسائل التواصل الإجتماعي' : 'Social Media',
      });
    }

    if (socials.tiktok) {
      backfields.push({
        key: 'TikTok',
        value: socials.tiktok,
        label: 'TikTok',
        row: socialRow++,
        section: language === 'ar' ? 'وسائل التواصل الإجتماعي' : 'Social Media',
      });
    }

    if (socials.whatsapp) {
      backfields.push({
        key: 'WhatsApp',
        value: socials.whatsapp,
        label: 'WhatsApp',
        row: socialRow++,
        section: language === 'ar' ? 'وسائل التواصل الإجتماعي' : 'Social Media',
      });
    }

    if (socials.snapchat) {
      backfields.push({
        key: 'Snapchat',
        value: socials.snapchat,
        label: 'Snapchat',
        row: socialRow++,
        section: language === 'ar' ? 'وسائل التواصل الإجتماعي' : 'Social Media',
      });
    }
  }

  // Add Catalogue if available
  if (catalogue) {
    backfields.push({
      key: 'Catalogue',
      value: catalogue,
      label: language === 'ar' ? 'الكتالوج' : 'Catalogue',
      row: 0,
      section: language === 'ar' ? 'الكتالوج' : 'Catalogue',
    });
  }
};

// Localized front fields
const localizedFrontFields = {
  'Points': {
    'ar': 'النقاط',
    'en': 'Points'
  },
  'Rewards': {
    'ar': 'المكافآت',
    'en': 'Rewards'
  },
  'Tier': {
    'ar': 'المستوى',
    'en': 'Tier'
  },
  'Name': {
    'ar': 'الاسم',
    'en': 'Name'
  },
  'Benefits': {
    'ar': 'المزايا',
    'en': 'Benefits'
  }
};

const processCSV = async () => {
  const customers = [];

  // Step 1: Read the CSV file
  fs.createReadStream(csvFilePath)
    .pipe(csvParser())
    .on('data', (row) => {
      customers.push(row);
    })
    .on('end', async () => {
      console.log('CSV file successfully processed');
      console.log(customers, 'customers');
      // Connect to MongoDB
      try {
        await client.connect();
        const db = client.db('boonus');
        const customerLoyaltyCollection = db.collection('customerloyalties');
        const walletCollection = db.collection('wallets');
        const businessCollection = db.collection('businesses');
        const customerCollection = db.collection('customers');
        const customerVoucherCollection = db.collection('customervouchers');

        const businessData = await businessCollection
          .aggregate([{ $match: { _id: businessId } }])
          .toArray();

        console.log(businessData[0].name, 'businessData');
        // Fetch Wallet Information (aggregation)
        const walletData = await walletCollection
          .aggregate([
            { $match: { businessId, _id: walletId } },
            { $project: { icon: 1, logo: 1, colors: 1, default: 1, businessId: 1 } },
          ])
          .toArray();

        if (!walletData.length) {
          console.error(`No wallet found for businessId: ${businessId}`);
          return;
        }

        const wallet = walletData[0];

        // Process each customer
        for (const customer of customers) {
          const phoneNumber = customer.phone;

          const localPhoneNumber = phoneNumber.startsWith('+966')
            ? phoneNumber.slice(4)
            : phoneNumber;
          
          // Fetch Customer Loyalty Information (aggregation)
          const loyaltyData = await customerLoyaltyCollection
            .aggregate([
              { $match: { 'phone.number': localPhoneNumber, businessId } },
              { $project: { points: 1, description: 1, serialNumbers: 1 } },
            ])
            .toArray();

          if (!loyaltyData.length) {
            console.log(
              `No loyalty record found for phone: ${localPhoneNumber}`
            );
            continue;
          }

          // Get customer details
          const customerData = await customerCollection.findOne({
            'phone.number': localPhoneNumber
          });

          if (!customerData) {
            console.log(`No customer record found for phone: ${localPhoneNumber}`);
            continue;
          }

          // Get rewards
          const rewards = await customerVoucherCollection.find(
            {
              redeemed: false,
              businessId,
              owner: customerData._id,
              voucherSource: POINTS,
            },
            { projection: { _id: 0, name: 1, arabicName: 1, code: 1 } }
          ).toArray();

          // Get campaign rewards
          const campaignRewards = await customerVoucherCollection.find(
            {
              businessId,
              redeemed: false,
              owner: customerData._id,
              voucherSource: CAMPAIGN,
              walletId,
            },
            { projection: { _id: 0, name: 1, arabicName: 1, code: 1, campaignId: 1, claimable: 1 } }
          ).toArray();

          // Count redeemed rewards
          const redeemedRewardsCount = await customerVoucherCollection.countDocuments({
            redeemed: true,
            businessId,
            owner: customerData._id,
            $or: [{ voucherSource: POINTS }, { voucherSource: CAMPAIGN }],
          });

          // Get branches information
          const branches = [];
          if (businessData[0].branches && businessData[0].branches.length > 0) {
            businessData[0].branches.forEach(branch => {
              if (branch.status === 'active') {
                branches.push({
                  name: branch.name,
                  address: branch.address
                });
              }
            });
          }

          console.log(loyaltyData, 'loyaltyData');
          const loyalty = loyaltyData[0];
          console.log(loyalty, 'loyalty');
          console.log(customer, 'customer');

          // Prepare payload
          const payload = {
            serial_number: loyalty?.serialNumbers[0]?.serialNumber,
            business_name: businessData[0].name,
            business_id: businessId.toString(),
            wallet_id: walletId.toString(),
            last_update: `Congrats! 🥳. New Points Added!!! You Now Have ${customer.points} Points`,
            msg: 'has been updated',
            icon: wallet.icon,
            logo: wallet.logo,
            logo_text: businessData[0].name || 'Default Business',
            strip_image_color: wallet.colors?.stripColor,
            strip_image_url: wallet.colors?.stripImage || null,
            qrCodeData: {
              customer_name: customer.name,
              customer_mobile_number: customer.phone
                .split('')
                .slice(4)
                .join(''),
              mobile_country_code: '966',
            },
            card_background: wallet.colors?.cardBackground,
            card_foreground:
              wallet.colors?.cardForeground || wallet.colors?.cardText,
            card_text_color: wallet.colors?.cardText,
            description: loyalty.description || 'Default Description',
            default_wallet: wallet.default || false,
            backfields: [],
            front_fields: [
              {
                key: 'Points',
                value: Number(customer.points),
                label: 'Points',
                is_header: true,
              },
              {
                key: 'rewards',
                value: rewards.length + campaignRewards.length,
                label: 'Rewards',
              },
            ],
            expired: false,
          };

          // Add backfields
          const language = customerData.settings?.language || 'en';
          
          addPassBackfields(
            payload.backfields,
            customerData,
            null, // feedbackLink
            redeemedRewardsCount,
            businessData[0].loyalty?.terms || '', // terms
            [], // rewardsReadyToClaim
            rewards,
            campaignRewards,
            payload.last_update,
            POINTS,
            language,
            branches,
            businessData[0].socials || {},
            businessData[0].catalogue,
            {
              email: businessData[0].supportEmail,
              phone: businessData[0].supportPhone?.number && businessData[0].supportPhone?.countryCode
                ? `+${businessData[0].supportPhone.countryCode}${businessData[0].supportPhone.number}`
                : null,
            },
            null, // tierPerks
            loyalty.description || 'Default Description',
            businessData[0].name,
            wallet.default || false,
            businessData[0].website
          );

          console.log(payload, 'payload');

          // Step 3: Send data to the API
          try {
            const response = await axios.post(
              'https://passes.boonus.app/api/v1/passes/update-points-pass?lang=' + language,
              payload,
              {
                headers: { 'Content-Type': 'application/json' },
              }
            );
            console.log('API Response:', response.data);
          } catch (error) {
            console.error('Error sending data to API:', error.message);
          }
        }
      } catch (error) {
        console.error('Error processing CSV:', error.message);
      } finally {
        await client.close();
      }
    });
};

processCSV();
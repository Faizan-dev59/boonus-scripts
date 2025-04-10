// import moment from 'moment';
// import { generateVoucherCode } from '../../helpers/helper';
// import config from '../../../../config';
// import * as constants from './constants';
// import CustomerVoucher from '../../models/customerVoucher';
// import Wallet from '../../models/wallet';
// import Campaign from '../../models/campaign';
// import Loyalty from '../../models/loyalty';
// import CustomerLoyalty from '../../models/customerLoyalty';
// import plan from '../../models/plan';
// import type { CampaignJob } from '../../../../workers/campaigns-processor';
// import { campaignQ1 } from '../../../../workers/campaigns';
// import { log } from '../../../../logger';
// import mongoose from 'mongoose';

// /**
//  * this function is responsible for generating rewards when sending campaign
//  *
//  * @param {object} reward
//  * @param {string} campaignId
//  * @param {string} walletId
//  * @param {object} customerLoyalty
//  * @param {object} business
//  * @returns {void}
//  */
// export const generateCampaignReward = async (
//   reward: {
//     discount: any;
//     rewardCost: string;
//     products: any;
//     name: string;
//     arabicName: string;
//   },
//   campaignId: string,
//   walletId: string,
//   customerLoyalty: { customerId: string },
//   business: { loyaltyId: string; _id: string; category: string }
// ) => {
//   await CustomerVoucher.create({
//     code: await generateVoucherCode({
//       pattern: '########',
//       length: 8,
//       charset: config.vouchersCharSet,
//     }),
//     discount: reward.discount,
//     loyaltyId: business.loyaltyId,
//     businessId: business._id,
//     owner: customerLoyalty.customerId,
//     category: business.category,
//     voucherSource: constants.CAMPAIGN,
//     campaignId,
//     walletId,
//     name: reward.name,
//     arabicName: reward.arabicName,
//     products: reward.products,
//     rewardCost: reward.rewardCost,
//   });
// };
// type Limit = {
//   availablePushNotification: number;
//   customerLoyaltyList: any[];
// };
// /**
//  * this function is responsible for checking notifications limit and return available push notification
//  * @param {array} customerLoyaltyList
//  * @param {object} business
//  * @returns {Promise<Limit>}
//  */
// export const checkNotificationLimit = async (
//   customerLoyaltyList: any[],
//   business: {
//     pushNotificationLimit: number;
//     pushNotificationUsage: number;
//     planId: string;
//   }
// ): Promise<Limit> => {
//   let availablePushNotification;
//   if (business?.planId) {
//     const businessPlan = await plan.findOne({ _id: business.planId });
//     availablePushNotification =
//       businessPlan.pushNotificationLimit - business.pushNotificationUsage;
//   } else {
//     availablePushNotification =
//       business.pushNotificationLimit - business.pushNotificationUsage;
//   }
//   return {
//     availablePushNotification,
//     customerLoyaltyList: customerLoyaltyList.slice(
//       0,
//       availablePushNotification
//     ),
//   };
// };

// /**
//  * this function is responsible for handle automated campaigns (find customers needed to get update and generate rewards)
//  * @param {string} trigger
//  * @returns {void}
//  */
// export const handleAutomatedCampaigns = async (
//   trigger: 'LAST_VISIT' | 'BIRTHDAY'
// ) => {
//   try {
//     log.info(`Starting automated campaigns with trigger: ${trigger}`);
//     const campaigns = await Campaign.find({
//       trigger,
//       active: true,
//     }).populate(
//       'businessId',
//       'live pushNotificationLimit pushNotificationUsage planId loyaltyId category branches name arabicName logo website supportEmail supportPhone socials catalogue deleted'
//     );
//     log.info(
//       `Found ${campaigns.length} active campaigns for trigger: ${trigger}`
//     );

//     for (const campaign of campaigns) {
//       const { trigger, triggerValue } = campaign;
//       const business = campaign.businessId;
//       const businessId = campaign.businessId._id;
//       log.info(
//         `Processing campaign ID: ${campaign?._id} for business ID: ${campaign.businessId._id}`
//       );

//       // Skip inactive or deleted businesses
//       if (!campaign.businessId.live) {
//         log.warn(`Business ${campaign.businessId._id} is not live. Skipping.`);
//         continue;
//       }
//       if (campaign?.businessId?.deleted) {
//         log.warn(
//           `Business ${campaign.businessId._id} is marked as deleted. Skipping.`
//         );
//         continue;
//       }

//       const wallet = await Wallet.findOne({
//         default: true,
//         businessId,
//       });
//       if (!wallet) {
//         log.error(
//           `No default wallet found for business ID: ${campaign.businessId._id}`
//         );
//         continue;
//       }
//       const loyalty = await Loyalty.findOne(
//         { businessId },
//         {
//           businessId: 1,
//           rewards: 1,
//           description: 1,
//           arabicDescription: 1,
//           howToUse: 1,
//           arabicHowToUse: 1,
//           terms: 1,
//           arabicTerms: 1,
//           levels: 1,
//           membershipProgram: 1,
//           name: 1,
//           arabicName: 1,
//         }
//       );
//       if (!loyalty) {
//         log.error(
//           `No loyalty program found for business ID: ${campaign.businessId._id}`
//         );
//         continue;
//       }
//       let customerLoyaltyList;
//       if (trigger === constants.BIRTHDAY) {
//         const currentDay = moment().tz('Asia/Riyadh').date();
//         const currentMonth = moment().tz('Asia/Riyadh').month() + 1;
//         log.info(
//           `Looking for customers with birthdays today: ${currentDay}-${currentMonth}`
//         );
//         // Find all customers whose birthday is today
//         customerLoyaltyList = await CustomerLoyalty.aggregate([
//           { $match: { 'serialNumbers.walletId': wallet._id } },
//           {
//             $lookup: {
//               from: 'customers',
//               let: { customerId: '$customerId' },
//               pipeline: [
//                 {
//                   $match: {
//                     $expr: {
//                       $and: [
//                         { $eq: [{ $dayOfMonth: '$birthDate' }, currentDay] },
//                         { $eq: [{ $month: '$birthDate' }, currentMonth] },
//                         { $eq: ['$$customerId', '$_id'] },
//                       ],
//                     },
//                   },
//                 },
//               ],
//               as: 'customer',
//             },
//           },
//           { $unwind: '$customer' },
//         ]);
//         log.info(
//           `Found ${customerLoyaltyList.length} customers with birthdays today.`
//         );
//       } else if (trigger === constants.LAST_VISIT) {
//         // find all customers there last visit was >= LAST_VISIT triggerValue filtered who already got this campaign
//         const daysAgo = moment()
//           .startOf('day')
//           .subtract(triggerValue, 'days')
//           .toDate();
//         log.info(`Looking for customers who last visited before: ${daysAgo}`);
//         const fetchCustomerLoyaltyList = async () => {
//           const customerLoyaltyList = await CustomerLoyalty.aggregate([
//             {
//               $match: {
//                 'serialNumbers.walletId': wallet._id,
//                 customerId: { $nin: campaign.reachList },
//                 lastVisit: { $lte: daysAgo },
//                 businessId,
//               },
//             },
//             {
//               $project: {
//                 customerId: 1,
//                 lastVisit: 1,
//                 serialNumbers: 1,
//                 points: 1,
//                 currentLevel: 1,
//               },
//             },
//             {
//               $sort: {
//                 lastVisit: 1,
//               },
//             },
//           ]);

//           log.info(
//             `Found ${customerLoyaltyList.length} customers with last visit >= ${triggerValue} days ago.`
//           );

//           return customerLoyaltyList;
//         };
//         customerLoyaltyList = await fetchCustomerLoyaltyList();
//       }
//       log.info(
//         `Attempt to send ${campaign.trigger} campaign with id: ${campaign._id} for business: ${campaign.businessId._id}`
//       );
//       // check limit and update usage
//       const limitResult: {
//         availablePushNotification: number;
//         customerLoyaltyList: any[];
//       } = await checkNotificationLimit(
//         customerLoyaltyList,
//         campaign.businessId
//       );
//       log.info(
//         `Available push notifications: ${limitResult.availablePushNotification}`
//       );
//       if (limitResult.availablePushNotification === 0) {
//         log.warn(
//           `No available push notifications for campaign ID: ${campaign?._id}`
//         );
//         continue;
//       }
//       customerLoyaltyList = limitResult.customerLoyaltyList;
//       const campaignJobData: CampaignJob = {
//         campaignId: campaign._id,
//         customersList: customerLoyaltyList,
//         campaignReward: campaign.reward,
//         loyalty,
//         business: business,
//         campaignWalletId: wallet._id,
//         arabicCTAMessage: campaign.arabicCTAMessage,
//         CTAMessage: campaign.CTAMessage,
//         wallet,
//       };
//       // generate reward and send wallet update
//       if (customerLoyaltyList.length) {
//         const jobName = String(`${campaign._id}`);
//         log.info(
//           `Adding job ${jobName} to queue with ${customerLoyaltyList.length} customers.`
//         );
//         await campaignQ1.add(jobName, campaignJobData, {
//           removeOnComplete: true,
//           removeOnFail: {
//             age: 24 * 3600, // keep up to 24 hours
//           },
//           priority: 5,
//           delay: 5000, //5 sec
//         });
//         log.info(`Job ${jobName} added to queue ${campaignQ1.name}`);
//       }
//       await campaign.save();
//     }
//   } catch (error) {
//     log.error(`Error processing automated campaigns: ${error}`);
//   }
// };

/*
 * This code is a protected work and subject to domestic and international copyright
 * law(s). A complete listing of authors of this work is readily available. Additionally,
 * source code is, by its very nature, confidential information and inextricably contains
 * trade secrets and other information proprietary, valuable and sensitive to Redknee. No
 * unauthorized use, disclosure, manipulation or otherwise is permitted, and may only be
 * used in accordance with the terms of the license agreement entered into with Redknee
 * Inc. and/or its subsidiaries.
 *
 * Copyright (c) Redknee Inc. and its subsidiaries. All Rights Reserved.
 */
package com.redknee.app.crm.mediation.ipcg;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import com.redknee.framework.xhome.context.AgentException;
import com.redknee.framework.xhome.context.Context;
import com.redknee.framework.xhome.home.HomeException;
import com.redknee.framework.xhome.util.time.Time;

import com.redknee.app.crm.bean.CallType;
import com.redknee.app.crm.bean.CallTypeHome;
import com.redknee.app.crm.bean.CallTypeID;
import com.redknee.app.crm.bean.IPCGData;
import com.redknee.app.crm.bean.calldetail.CallDetail;
import com.redknee.app.crm.bean.calldetail.RateUnitEnum;
import com.redknee.app.crm.bean.calldetail.TeleserviceTypeEnum;
import com.redknee.app.crm.config.IPCGPollerConfig;
import com.redknee.app.crm.mediation.bean.Account;
import com.redknee.app.crm.mediation.bean.AccountHome;
import com.redknee.app.crm.mediation.bean.Subscriber;
import com.redknee.app.crm.mediation.poller.ProcessorInfo;
import com.redknee.app.crm.mediation.poller.event.CRMProcessorSupport;
import com.redknee.app.crm.mediation.report.ReportUtilities;
import com.redknee.app.crm.mediation.support.SpidSupport;
import com.redknee.app.crm.mediation.support.SubscriberSupport;
import com.redknee.util.snippet.log.Logger;


/**
 * Creates the call details for IPCG.
 *
 * @author arturo.medina@redknee.com
 * @author cindy.wong@redknee.com
 */
public class IPCGCallDetailCreator implements CallDetailCreator
{

    /**
     * Create a new instance of <code>IPCGCallDetailCreator</code>.
     */
    private IPCGCallDetailCreator()
    {
        // empty
    }


    /**
     * Returns an instance of <code>IPCGCallDetailCreator</code>.
     *
     * @return An instance of <code>IPCGCallDetailCreator</code>.
     */
    public static IPCGCallDetailCreator instance()
    {
        if (instance == null)
        {
            instance = new IPCGCallDetailCreator();
        }
        return instance;
    }


    /**
     * {@inheritDoc}
     */
    public CallDetail createCallDetail(final Context ctx, final ProcessorInfo info, final List<String> params)
            throws ParseException, HomeException, AgentException
    {
        /*
         * IPCG always returns a collection of call details so this method doesn't make
         * any sense to be implemented in the IPCG call detail creator.
         */
        return null;
    }


    /**
     * {@inheritDoc}
     */
    public List<CallDetail> createCallDetails(final Context ctx, final ProcessorInfo info, final List<String> params)
            throws ParseException, HomeException, AgentException
    {
        final List<CallDetail> list = new ArrayList<CallDetail>();
        CallDetail cd = null;
        try
        {
            CRMProcessorSupport.makeArray(ctx, params, info.getRecord(), info.getStartIndex(), ',', info.getErid(),
                    this);
        }
        catch (Throwable th)
        {
            throw new AgentException("The ER is filter out");
        }
        for (final IPCGData data : IPCGWUnifiedBillingParser.instance().processEr501(ctx, params, false))
        {
            cd = createCallDetail(ctx, data, false);
            list.add(cd);
        }
        return list;
    }


    /**
     * Creates a call detail based on an IPCGData buffer item.
     *
     * @param ctx
     *            The operating context.
     * @param data
     *            IPCGData buffer item.
     * @return Corresponding call detail.
     * @throws HomeException
     *             Thrown if there are problems creating the call detail.
     */
    public CallDetail createCallDetail(final Context ctx, final IPCGData data) throws HomeException
    {
        return createCallDetail(ctx, data, false);
    }


    /**
     * Creates a call detail based on an IPCGData buffer item.
     *
     * @param ctx
     *            The operating context.
     * @param data
     *            IPCGData buffer item.
     * @param logError
     *            Whether call detail generation errors are logged to the appropriate
     *            error file.
     * @return Corresponding call detail.
     * @throws HomeException
     *             Thrown if there are problems creating the call detail.
     */
    public CallDetail createCallDetail(final Context ctx, final IPCGData data, final boolean logError)
            throws HomeException
    {
        String ban = null;

        final Subscriber subscriber = SubscriberSupport.lookupSubscriberForSubId(ctx, data.getSubscriberId());

        if(subscriber != null)
        {
            ban = subscriber.getBAN();
            ctx.put(Subscriber.class, subscriber);
        }

        if (ban == null || ban.trim().length() == 0)
        {
            final String message = MessageFormat.format("Cannot find account for subscriber \"{0}\". Cannot continue.",
                    data.getChargedMSISDN());
            Logger.major(ctx, this, message);
            throw new HomeException(message);
        }

        final Account acct = (Account) ReportUtilities.findByPrimaryKey(ctx, AccountHome.class, ban);
        if (acct == null)
        {
            if (logError)
            {
                writeError(ctx, ban, data.getChargedMSISDN());
            }
            throw new HomeException("Invalid account: " + ban);
        }

        final CallTypeID callTypeId = new CallTypeID(data.getCallType().getIndex(), acct.getSpid());
        final CallType callType = (CallType) ReportUtilities.findByPrimaryKey(ctx, CallTypeHome.class, callTypeId);
        if (callType == null)
        {
            final String message = MessageFormat.format(
                    "Could not find CallType entry for type id \"{0}\" and service provider \"{1}\".", String
                            .valueOf(data.getCallType().getIndex()), String.valueOf(acct.getSpid()));
            Logger.major(ctx, this, message);
            throw new HomeException(message);
        }

        if (RateUnitEnum.SEC == data.getUnitType() || RateUnitEnum.MIN == data.getUnitType())
        {
            final Time duration = new Time();
            duration.set(0, 0, (int) data.getUsage(), 0);
        }
        /*
         *  There can be max of 3 glCodes
         *  TT#12111334023
         */
        StringTokenizer glCodes = new StringTokenizer(data.getGlCode(), "|");
		callDetail = autofix0callDetail(ctx, data, ban, subscriber, acct, callType, duration, glCodes);
        return callDetail;
    }


    /** This will parse the glCodes Ex. 123|456|789 and
     *  put them into respective GLCodes field in callDetail table.
     * @param callDetail
     * @param glCodes
     *
     */
    private void setComponentGLCodes(CallDetail callDetail, StringTokenizer glCodes) {
        String str = null;
        if (glCodes.hasMoreTokens()) {
            str = glCodes.nextToken();
            if (!str.trim().equals("")) {
                callDetail.setComponentGLCode1(str);

            }
        }
        if (glCodes.hasMoreTokens()) {
            str = glCodes.nextToken();
            if (!str.trim().equals("")) {
                callDetail.setComponentGLCode2(str);
            }
        }

        if (glCodes.hasMoreTokens()) {
            str = glCodes.nextToken();
            if (!str.trim().equals("")) {
                callDetail.setComponentGLCode3(str);
            }
        }
        // pick first GL Code from 3 components
        if (callDetail.getComponentGLCode1() != null && !callDetail.getComponentGLCode1().trim().equals("") ) {
            callDetail.setGLCode(callDetail.getComponentGLCode1());
        } else if (callDetail.getComponentGLCode2() != null && !callDetail.getComponentGLCode2().trim().equals("") ) {
            callDetail.setGLCode(callDetail.getComponentGLCode2());
        } else if (callDetail.getComponentGLCode3() != null && !callDetail.getComponentGLCode3().trim().equals("") ) {
            callDetail.setGLCode(callDetail.getComponentGLCode3());
        }
    }




    /**
     * Logs a call detail error to the IPCG error log file.
     *
     * @param context
     *            The operating context.
     * @param ban
     *            Account BAN associated with the error.
     * @param msisdn
     *            MSISDN associated with the error.
     */
    public synchronized void writeError(final Context context, final String ban, final String msisdn)
    {
        try
        {
            if (this.errorStream_ == null)
            {
                final IPCGPollerConfig config = (IPCGPollerConfig) context.get(IPCGPollerConfig.class);
                this.errorStream_ = new PrintStream(new FileOutputStream(new File(config.getErrorLog())));
            }
            this.errorStream_.printf("BAN = %s, MSISDN = %s", ban, msisdn);
        }
        catch (final Exception exception)
        {
            final StringBuilder sb = new StringBuilder();
            sb.append(exception.getClass().getSimpleName());
            sb.append(" caught in ");
            sb.append("IPCGCallDetailCreator.writeError(): ");
            if (exception.getMessage() != null)
            {
                sb.append(exception.getMessage());
            }
            Logger.minor(context, this, sb.toString(), exception);
        }
    }

    /**
     * Error print stream.
     */
    private PrintStream errorStream_;
    /**
     * Singleton instance.
     */
    private static IPCGCallDetailCreator instance;
    public final static String DEFAULT_TABLE_NAME = "XIPCGData";
	private CallDetail autofix0callDetail(Context ctx, IPCGData data, String ban, Subscriber subscriber, Account acct,
			CallType callType, Time duration, StringTokenizer glCodes) {
		if (subscriber != null) {
			ban = subscriber.getBAN();
		}
		final CallDetail callDetail = new CallDetail();
		callDetail.setBAN(ban);
		callDetail.setSubscriberID(subscriber.getId());
		callDetail.setTranDate(data.getTranDate());
		callDetail.setCallType(data.getCallType());
		callDetail.setPostedDate(new Date());
		callDetail.setChargedMSISDN(data.getChargedMSISDN());
		if (RateUnitEnum.SEC == data.getUnitType() || RateUnitEnum.MIN == data.getUnitType()) {
			callDetail.setDuration(duration);
		} else {
			callDetail.setDataUsage(data.getUsage());
		}
		callDetail.setVariableRateUnit(data.getUnitType());
		callDetail.setCharge(Math.round(data.getCharge() / 10.0));
		callDetail.setSpid(acct.getSpid());
		callDetail.setTaxAuthority1(SpidSupport.getDataTaxAuthority(ctx, acct.getSpid()));
		if (data.getGlCode() == null || data.getGlCode().trim().equals("") || glCodes.countTokens() == 0) {
			callDetail.setGLCode(callType.getGLCode());
		} else {
			setComponentGLCodes(callDetail, glCodes);
		}
		callDetail.setComponentCharge1(data.getComponentCharge1());
		callDetail.setComponentCharge2(data.getComponentCharge2());
		callDetail.setComponentCharge3(data.getComponentCharge3());
		callDetail.setBillingOption(data.getBillingOption());
		callDetail.setCallID(Long.toString(data.getIPCGDataID()));
		callDetail.setLocationCountry(data.getLocationCountry());
		callDetail.setLocationOperator(data.getLocationOperator());
		callDetail.setApn(data.getApn());
		callDetail.setTeleserviceType(TeleserviceTypeEnum.DATA);
		callDetail.setHomeProv(callDetail.getChargedMSISDN());
		callDetail.setSecondaryBalanceIndicator(data.getSecondaryBalanceIndicator());
		callDetail.setSecondaryBalanceChargedAmount(data.getSecondaryBalanceChargedAmount());
		callDetail.setRatingRule(data.getRateRuleId());
		return callDetail;
	}
}

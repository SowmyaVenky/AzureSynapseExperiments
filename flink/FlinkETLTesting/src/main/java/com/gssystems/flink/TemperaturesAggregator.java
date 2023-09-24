package com.gssystems.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import com.google.gson.Gson;

public class TemperaturesAggregator implements MapFunction<String, Tuple2<String, TemperatureAggregateBean>> {
	private static final long serialVersionUID = 1L;

	public Tuple2<String, TemperatureAggregateBean> map(String value) {
		Gson gs = new Gson();
		TemperaturesOutBean bean = gs.fromJson(value, TemperaturesOutBean.class);
		double lat = bean.getLatitude();
		double lng = bean.getLongitude();
		String time = bean.getTime();
		String year = time.substring(0, 4);
		String month = time.substring(5, 7);
		double temperature = bean.getTemperature_2m();

		StringBuffer b = new StringBuffer();
		b.append(lat);
		b.append(":");
		b.append(lng);
		b.append(":");
		b.append(year);
		b.append(":");
		b.append(month);

		TemperatureAggregateBean aBean = new TemperatureAggregateBean();
		aBean.setLat(lat);
		aBean.setLng(lng);
		aBean.setMaxTemp(temperature);
		aBean.setMinTemp(temperature);
		aBean.setYear(year);
		aBean.setMonth(month);
		aBean.setCount(1);

		Tuple2<String, TemperatureAggregateBean> toRet = new Tuple2<String, TemperatureAggregateBean>(b.toString(),
				aBean);
		return toRet;
	}
}
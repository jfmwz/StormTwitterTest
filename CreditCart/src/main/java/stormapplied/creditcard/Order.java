package stormapplied.creditcard;

import java.io.Serializable;

public class Order implements Serializable {
    private long id;
    private long customerId;
    private long creditCardNumber;
    private String creditCardExpiration;
    private int creditCardCode;
    private double chargeAmount;

    public Order() {

    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", customerId=" + customerId +
                ", creditCardNumber=" + creditCardNumber +
                ", creditCardExpiration='" + creditCardExpiration + '\'' +
                ", creditCardCode=" + creditCardCode +
                ", chargeAmount=" + chargeAmount +
                '}';
    }
}